# Module 5: Gen AI — Clinical Intelligence

| Duration | 45 minutes |
|----------|------------|
| Objective | Use Azure OpenAI through Fabric to summarize clinical notes, extract medical entities, and suggest ICD-10 codes — automating documentation tasks that consume hours of clinician time |
| Fabric Features | Fabric Notebook, Azure OpenAI integration, PySpark UDFs |

---

## Why Gen AI for Clinical Documentation?

Clinicians spend **~2 hours on documentation for every 1 hour of patient care.** Administrative burden is the #1 driver of physician burnout (AHA, 2026). Gen AI can dramatically reduce this burden by:

- **Summarizing** lengthy clinical notes into concise discharge summaries
- **Extracting** diagnoses, medications, and procedures automatically
- **Suggesting** ICD-10 codes from free-text narratives (improving coding accuracy and revenue capture)
- **Identifying** key clinical facts across thousands of notes in seconds

In this module, you'll build an AI-powered clinical intelligence pipeline that processes the free-text clinical notes in your Lakehouse.

---

## Prerequisites

To complete this module, you need:

1. **Azure OpenAI resource** with a deployed model (e.g., `gpt-4o`, `gpt-4o-mini`, or `gpt-35-turbo`)
2. The **endpoint URL** and **API key** for your Azure OpenAI deployment
3. The **deployment name** of your model

> **If you don't have Azure OpenAI access:** You can still read through the module to understand the approach. Instructors may provide a shared endpoint for the lab.

---

## What You Will Do

1. Configure Azure OpenAI connection in a notebook
2. Summarize clinical notes using GPT
3. Extract medical entities (diagnoses, medications, procedures)
4. Suggest ICD-10 codes from clinical text
5. Store AI-generated insights back in the Lakehouse

---

## Part A: Set Up Azure OpenAI Connection

### Step 1: Create a New Notebook

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename to: `06 - GenAI Clinical Intelligence`
4. Attach the notebook to your `HealthcareLakehouse`

### Step 2: Configure the Connection

Paste in Cell 1:

```python
# =============================================================
# Cell 1: Azure OpenAI Configuration
# =============================================================

# ⚠️ Replace these values with your Azure OpenAI resource details
#
# IMPORTANT: The endpoint must be ONLY the base URL — do NOT include
# any path like /openai/v1 or /openai/deployments/...
# The SDK appends the correct path automatically.
#
# ✅ Correct:   https://my-resource.openai.azure.com/
# ❌ Wrong:     https://my-resource.openai.azure.com/openai/v1
# ❌ Wrong:     https://my-resource.openai.azure.com/openai/deployments/gpt-4o

AZURE_OPENAI_ENDPOINT = "https://<your-resource-name>.openai.azure.com/"
AZURE_OPENAI_KEY = "<your-api-key>"
AZURE_OPENAI_DEPLOYMENT = "<your-deployment-name>"  # e.g., "gpt-4o-mini"
AZURE_OPENAI_API_VERSION = "2024-06-01"

print("✅ Configuration set!")
print(f"   Endpoint: {AZURE_OPENAI_ENDPOINT[:40]}...")
print(f"   Deployment: {AZURE_OPENAI_DEPLOYMENT}")
```

> **Security Note:** In a production environment, use Azure Key Vault or Fabric environment variables instead of hardcoding credentials. For this lab, hardcoded values are acceptable.

### Step 3: Install the OpenAI SDK

Paste in Cell 2:

```python
# =============================================================
# Cell 2: Install OpenAI SDK
# =============================================================

%pip install openai -q
```

> **Expected warnings — safe to ignore:**
> - `ERROR: pip's dependency resolver...` — This is a pre-installed Fabric package (`nni`) with a stale dependency constraint. It does **not** affect the `openai` package or this lab.
> - `A new release of pip is available` — Informational only.
> - `PySpark kernel has been restarted` — Expected. Fabric restarts the kernel after `%pip install` so the new package is available. **Wait for the restart to complete, then continue with the next cell.**

### Step 4: Initialize the Client

Paste in Cell 3:

```python
# =============================================================
# Cell 3: Initialize Azure OpenAI Client
# =============================================================

from openai import AzureOpenAI

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

# Quick test
response = client.chat.completions.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    messages=[{"role": "user", "content": "Say 'Connection successful' if you can read this."}],
    max_tokens=10
)

print(response.choices[0].message.content)
```

You should see: `Connection successful`

---

## Part B: Load Clinical Notes

### Step 5: Load Notes from the Lakehouse

Paste in Cell 4:

```python
# =============================================================
# Cell 4: Load Clinical Notes from Silver Layer
# =============================================================

# For schema-enabled lakehouses, use the 3-part name: lakehouse.schema.table
# If your lakehouse does NOT have schema enabled, you can use just the table name
df_notes = spark.sql("SELECT * FROM HealthcareLakehouse.dbo.silver_clinical_notes")

print(f"Total clinical notes: {df_notes.count()}")
df_notes.groupBy("note_type").count().show()

# Preview a sample note
sample_note = df_notes.filter("note_type = 'Discharge Summary'").first()
print(f"\n--- Sample Discharge Summary ---")
print(f"Patient: {sample_note['patient_id']}")
print(f"Date: {sample_note['note_date']}")
print(f"Note:\n{sample_note['note_text'][:800]}...")
```

---

## Part C: Clinical Note Summarization

### Step 6: Summarize Clinical Notes

Paste in Cell 5:

```python
# =============================================================
# Cell 5: Clinical Note Summarization
# =============================================================

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
            temperature=0.3  # Low temperature for factual, consistent output
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {str(e)}"

# Test with a sample note
sample = df_notes.filter("note_type = 'Discharge Summary'").first()

print("📋 ORIGINAL NOTE:")
print(sample["note_text"][:600])
print("\n" + "=" * 60)

summary = summarize_clinical_note(sample["note_text"], sample["note_type"])
print("\n🤖 AI SUMMARY:")
print(summary)
```

---

## Part D: Medical Entity Extraction

### Step 7: Extract Clinical Entities

Paste in Cell 6:

```python
# =============================================================
# Cell 6: Medical Entity Extraction
# =============================================================
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
            temperature=0.1  # Very low temperature for consistent extraction
        )
        result_text = response.choices[0].message.content.strip()
        # Remove markdown code fences if present
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[-1].rsplit("```", 1)[0]
        return json.loads(result_text)
    except json.JSONDecodeError:
        return {"error": "Failed to parse response as JSON", "raw": result_text}
    except Exception as e:
        return {"error": str(e)}

# Test entity extraction
sample = df_notes.filter("note_type = 'ED Note'").first()

print("📋 CLINICAL NOTE:")
print(sample["note_text"][:500])
print("\n" + "=" * 60)

entities = extract_medical_entities(sample["note_text"])
print("\n🏥 EXTRACTED ENTITIES:")
print(json.dumps(entities, indent=2))
```

---

## Part E: ICD-10 Code Suggestion

### Step 8: Suggest ICD-10 Codes

Paste in Cell 7:

```python
# =============================================================
# Cell 7: ICD-10 Code Suggestion
# =============================================================

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

# Test ICD-10 suggestion
sample = df_notes.filter("note_type = 'Discharge Summary'").first()

print("📋 CLINICAL NOTE (excerpt):")
print(sample["note_text"][:400])
print("\n" + "=" * 60)

codes = suggest_icd10_codes(sample["note_text"])
print("\n💊 SUGGESTED ICD-10 CODES:")
for code in codes:
    if "error" not in code:
        print(f"  {code.get('code', 'N/A'):10s} | {code.get('confidence', 'N/A'):6s} | {code.get('description', 'N/A')}")
        print(f"{'':10s}   Evidence: {code.get('evidence', 'N/A')}")
        print()
```

---

## Part F: Process Notes at Scale

### Step 9: Batch Process All Clinical Notes

Paste in Cell 8:

```python
# =============================================================
# Cell 8: Batch Process Clinical Notes
# =============================================================
import time

# Collect notes to process (limit to a manageable batch for the lab)
notes_pdf = df_notes.limit(20).toPandas()

results = []

print(f"Processing {len(notes_pdf)} clinical notes...\n")

for idx, row in notes_pdf.iterrows():
    note_id = row["note_id"]
    patient_id = row["patient_id"]
    note_type = row["note_type"]
    note_text = row["note_text"]
    
    print(f"[{idx+1}/{len(notes_pdf)}] Processing {note_id} ({note_type})...", end=" ")
    
    # Generate summary
    summary = summarize_clinical_note(note_text, note_type)
    
    # Extract entities
    entities = extract_medical_entities(note_text)
    
    # Suggest ICD-10 codes
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
    
    # Small delay to avoid rate limiting
    time.sleep(0.5)

print(f"\n✅ Processed {len(results)} notes!")
```

### Step 10: Save AI Insights to the Lakehouse

Paste in Cell 9:

```python
# =============================================================
# Cell 9: Save AI-Generated Insights to Gold Layer
# =============================================================
from pyspark.sql.types import StructType, StructField, StringType

# Create schema
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

# Convert to Spark DataFrame and save as Delta table
df_ai_insights = spark.createDataFrame(results, schema=schema)

df_ai_insights.write.mode("overwrite").format("delta").saveAsTable("gold_clinical_ai_insights")

print(f"✅ Saved {df_ai_insights.count()} AI-enriched notes to gold_clinical_ai_insights")
df_ai_insights.select("note_id", "note_type", "ai_summary").show(5, truncate=60)
```

---

## Part G: Explore AI Insights

### Step 11: Query the Results

Paste in Cell 10:

```python
# =============================================================
# Cell 10: Explore AI-Generated Insights
# =============================================================

# Load the AI insights table
df_insights = spark.sql("SELECT * FROM HealthcareLakehouse.dbo.gold_clinical_ai_insights")

# Show summaries
print("=" * 60)
print("📋 AI-GENERATED CLINICAL SUMMARIES")
print("=" * 60)

for row in df_insights.limit(5).collect():
    print(f"\n🏷  Note: {row['note_id']} | Type: {row['note_type']} | Patient: {row['patient_id']}")
    print(f"   Summary: {row['ai_summary']}")
    print(f"   ICD-10 Codes: {row['suggested_icd10_codes']}")
    print("-" * 60)
```

Paste in Cell 11:

```python
# =============================================================
# Cell 11: ICD-10 Code Analysis
# =============================================================
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import ArrayType

# Parse the ICD-10 codes array and analyze distribution
df_codes = df_insights.select(
    "note_id",
    "patient_id",
    from_json(col("suggested_icd10_codes"), ArrayType(StringType())).alias("codes")
).select(
    "note_id",
    "patient_id",
    explode("codes").alias("icd10_code")
)

print("📊 Most Frequently Suggested ICD-10 Codes:")
df_codes.groupBy("icd10_code").count().orderBy("count", ascending=False).show(15)
```

---

## 💡 Discussion: AI in Clinical Settings

**Benefits:**
- Reduces documentation time by 50-70%
- Improves coding accuracy and revenue capture
- Surfaces critical findings that might be missed in long notes
- Enables trend analysis across thousands of patient notes

**Important Considerations:**
- AI-generated codes and summaries must be **reviewed by qualified clinicians** before use
- Models may hallucinate medical codes that don't exist
- Patient data must remain within compliant boundaries (Azure OpenAI with data privacy controls)
- Regular validation against expert-coded records is essential

**Discussion Questions:**
1. How would you validate the accuracy of AI-suggested ICD-10 codes?
2. What guardrails should exist before deploying this in a real clinical setting?
3. How could this pipeline integrate with an existing EHR system?

---

## ✅ Module 5 Checklist

Before moving to Module 6, confirm:

- [ ] Azure OpenAI connection is working (test call succeeded)
- [ ] Clinical note summarization produces concise, accurate summaries
- [ ] Entity extraction identifies diagnoses, medications, and procedures
- [ ] ICD-10 code suggestion returns plausible codes with evidence
- [ ] Batch processing completed for a set of notes
- [ ] AI insights are saved in the `gold_clinical_ai_insights` Delta table
- [ ] You can query and explore the AI-enriched data

---

**[← Module 4: Real-Time Analytics](Module04_RealTime_Analytics.md)** | **[Module 6: Prep Data for AI →](Module06_Prep_Data_for_AI.md)**
