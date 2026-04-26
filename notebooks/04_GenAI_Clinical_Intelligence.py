# =============================================================
# Notebook 04: Gen AI Clinical Intelligence
# 
# Purpose: Use Azure OpenAI to summarize clinical notes,
#          extract medical entities, and suggest ICD-10 codes
#
# Prerequisites:
#   - Azure OpenAI resource with a deployed model
#   - Endpoint URL, API key, and deployment name
#
# Instructions:
#   1. Create a notebook in Fabric named "06 - GenAI Clinical Intelligence"
#   2. Attach your HealthcareLakehouse
#   3. Replace the Azure OpenAI configuration values below
#   4. Run cells sequentially
# =============================================================

# --- Cell 1: Configuration ---
AZURE_OPENAI_ENDPOINT = "https://<your-resource-name>.openai.azure.com/"
AZURE_OPENAI_KEY = "<your-api-key>"
AZURE_OPENAI_DEPLOYMENT = "<your-deployment-name>"  # e.g., "gpt-4o-mini"
AZURE_OPENAI_API_VERSION = "2024-06-01"

print("✅ Configuration set!")
print(f"   Endpoint: {AZURE_OPENAI_ENDPOINT[:40]}...")
print(f"   Deployment: {AZURE_OPENAI_DEPLOYMENT}")

# --- Cell 2: Install SDK ---
# %pip install openai -q

# --- Cell 3: Initialize Client ---
from openai import AzureOpenAI

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

response = client.chat.completions.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    messages=[{"role": "user", "content": "Say 'Connection successful' if you can read this."}],
    max_tokens=10
)
print(response.choices[0].message.content)

# --- Cell 4: Load Clinical Notes ---
df_notes = spark.sql("SELECT * FROM HealthcareLakehouse.silver_clinical_notes")
print(f"Total clinical notes: {df_notes.count()}")
df_notes.groupBy("note_type").count().show()

# --- Cell 5: Summarize Clinical Notes ---
def summarize_clinical_note(note_text, note_type):
    """Use Azure OpenAI to create a concise summary of a clinical note."""
    system_prompt = """You are a clinical documentation specialist. Summarize the 
following clinical note into a concise 2-3 sentence summary that captures:
1. The patient's primary condition and presentation
2. Key findings or interventions
3. Disposition or next steps

Use medical terminology appropriately. Be concise and factual."""
    
    try:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Note Type: {note_type}\n\nClinical Note:\n{note_text}"}
            ],
            max_tokens=200,
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {str(e)}"

sample = df_notes.filter("note_type = 'Discharge Summary'").first()
print("📋 ORIGINAL NOTE:")
print(sample["note_text"][:600])
print("\n" + "=" * 60)
summary = summarize_clinical_note(sample["note_text"], sample["note_type"])
print("\n🤖 AI SUMMARY:")
print(summary)

# --- Cell 6: Medical Entity Extraction ---
import json

def extract_medical_entities(note_text):
    """Extract structured medical entities from a clinical note."""
    system_prompt = """You are a clinical NLP system. Extract the following entities 
from the clinical note and return them as a JSON object:

{
    "diagnoses": ["list of diagnoses mentioned"],
    "medications": ["list of medications mentioned"],
    "procedures": ["list of procedures or interventions mentioned"],
    "vitals_mentioned": ["any vital signs with values"],
    "allergies": ["any allergies mentioned"],
    "follow_up": "follow-up instructions if mentioned"
}

Only include entities that are explicitly mentioned in the text. 
Return valid JSON only, no other text."""
    
    try:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": note_text}
            ],
            max_tokens=500,
            temperature=0.1
        )
        result_text = response.choices[0].message.content.strip()
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[-1].rsplit("```", 1)[0]
        return json.loads(result_text)
    except json.JSONDecodeError:
        return {"error": "Failed to parse response as JSON", "raw": result_text}
    except Exception as e:
        return {"error": str(e)}

sample = df_notes.filter("note_type = 'ED Note'").first()
entities = extract_medical_entities(sample["note_text"])
print("🏥 EXTRACTED ENTITIES:")
print(json.dumps(entities, indent=2))

# --- Cell 7: ICD-10 Code Suggestion ---
def suggest_icd10_codes(note_text):
    """Suggest ICD-10 codes based on the clinical note content."""
    system_prompt = """You are a certified medical coder (CPC). Based on the 
clinical note provided, suggest the most appropriate ICD-10-CM codes.

Return a JSON array of objects with this format:
[
    {
        "code": "ICD-10 code",
        "description": "Official code description",
        "confidence": "high/medium/low",
        "evidence": "brief text from note supporting this code"
    }
]

Guidelines:
- Suggest the most specific code possible
- Include both primary and secondary diagnoses
- Only suggest codes with clear clinical evidence in the note
- Return valid JSON only, no other text"""
    
    try:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": note_text}
            ],
            max_tokens=600,
            temperature=0.2
        )
        result_text = response.choices[0].message.content.strip()
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[-1].rsplit("```", 1)[0]
        return json.loads(result_text)
    except json.JSONDecodeError:
        return [{"error": "Failed to parse", "raw": result_text}]
    except Exception as e:
        return [{"error": str(e)}]

sample = df_notes.filter("note_type = 'Discharge Summary'").first()
codes = suggest_icd10_codes(sample["note_text"])
print("💊 SUGGESTED ICD-10 CODES:")
for code in codes:
    if "error" not in code:
        print(f"  {code.get('code', 'N/A'):10s} | {code.get('confidence', 'N/A'):6s} | {code.get('description', 'N/A')}")

# --- Cell 8: Batch Process Notes ---
import time

notes_pdf = df_notes.limit(20).toPandas()
results = []

print(f"Processing {len(notes_pdf)} clinical notes...\n")

for idx, row in notes_pdf.iterrows():
    note_id = row["note_id"]
    patient_id = row["patient_id"]
    note_type = row["note_type"]
    note_text = row["note_text"]
    
    print(f"[{idx+1}/{len(notes_pdf)}] Processing {note_id} ({note_type})...", end=" ")
    
    summary = summarize_clinical_note(note_text, note_type)
    entities = extract_medical_entities(note_text)
    codes = suggest_icd10_codes(note_text)
    
    results.append({
        "note_id": note_id,
        "patient_id": patient_id,
        "note_type": note_type,
        "ai_summary": summary,
        "extracted_diagnoses": json.dumps(entities.get("diagnoses", [])),
        "extracted_medications": json.dumps(entities.get("medications", [])),
        "extracted_procedures": json.dumps(entities.get("procedures", [])),
        "suggested_icd10_codes": json.dumps([c.get("code", "") for c in codes if "error" not in c]),
        "suggested_icd10_details": json.dumps(codes)
    })
    
    print("✅")
    time.sleep(0.5)

print(f"\n✅ Processed {len(results)} notes!")

# --- Cell 9: Save to Lakehouse ---
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("note_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("note_type", StringType(), True),
    StructField("ai_summary", StringType(), True),
    StructField("extracted_diagnoses", StringType(), True),
    StructField("extracted_medications", StringType(), True),
    StructField("extracted_procedures", StringType(), True),
    StructField("suggested_icd10_codes", StringType(), True),
    StructField("suggested_icd10_details", StringType(), True)
])

df_ai_insights = spark.createDataFrame(results, schema=schema)
df_ai_insights.write.mode("overwrite").format("delta").saveAsTable("gold_clinical_ai_insights")

print(f"✅ Saved {df_ai_insights.count()} AI-enriched notes to gold_clinical_ai_insights")
df_ai_insights.select("note_id", "note_type", "ai_summary").show(5, truncate=60)
