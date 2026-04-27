# ================================================================
# NOTEBOOK 04: GEN AI — CLINICAL INTELLIGENCE
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 5 — GEN AI CLINICAL INTELLIGENCE                    │
# │  Fabric Capability: Azure OpenAI, PySpark, NLP Pipelines    │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "06 - GenAI Clinical Intelligence"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ⚠️ PREREQUISITES:
#   - Azure OpenAI resource with a deployed model (gpt-4o-mini recommended)
#   - Endpoint URL and API key from your Azure OpenAI deployment
#   - Silver layer tables populated (from Notebook 02)
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 🧠 Gen AI — Clinical Intelligence Pipeline
#
# ## The Documentation Burden in Healthcare
#
# Clinicians spend **~2 hours on documentation for every 1 hour 
# of patient care** (AHA, 2026). This administrative burden is 
# the #1 driver of physician burnout, contributing to:
# - 42% of physicians reporting burnout symptoms
# - $4.6 billion annual cost of physician turnover
# - Delayed care delivery due to documentation backlogs
#
# ## How Gen AI Helps
#
# Large Language Models (LLMs) like GPT-4o can process clinical 
# text to automate three critical documentation tasks:
#
# | Task | Manual Time | AI Time | Accuracy |
# |------|-------------|---------|----------|
# | Note summarization | 5-10 min/note | 3-5 sec | 90-95% |
# | Entity extraction | 10-15 min/note | 3-5 sec | 85-92% |
# | ICD-10 coding | 5-8 min/note | 3-5 sec | 80-90% |
#
# ## What We'll Build
#
# ```
# ┌──────────────┐     ┌───────────────┐     ┌──────────────┐
# │  Clinical     │────→│  Azure OpenAI  │────→│  Gold Layer   │
# │  Notes (Silver)│    │  (GPT-4o)      │    │  AI Insights  │
# └──────────────┘     └───────────────┘     └──────────────┘
#      150 notes         3 AI tasks:          Structured output
#                        • Summarize          ready for BI and
#                        • Extract entities   clinical review
#                        • Suggest ICD-10
# ```
#
# ## Important: AI as Assistant, Not Replacement
#
# All AI-generated outputs must be **reviewed by qualified 
# clinicians** before clinical use. This is a decision-support 
# tool, not an autonomous system. Key guardrails:
# - Temperature set LOW (0.1-0.3) for factual consistency
# - Structured JSON output format for reliable parsing
# - Confidence scoring on code suggestions
# - Human-in-the-loop review workflow


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Azure OpenAI Configuration                    ║
# ╚════════════════════════════════════════════════════════════════╝

# ⚠️ REPLACE these values with your Azure OpenAI resource details
# In production, use Azure Key Vault or Fabric environment variables
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


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — CODE: Install and Initialize OpenAI SDK             ║
# ╚════════════════════════════════════════════════════════════════╝

%pip install openai -q


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Initializing the Azure OpenAI Client
#
# We use the `AzureOpenAI` client from the official `openai` 
# Python SDK. Key configuration choices:
#
# - **API version `2024-06-01`**: Stable GA version with full 
#   chat completion support
# - **`azure_endpoint`**: Points to YOUR Azure OpenAI resource 
#   (not the public OpenAI API)
# - **Data residency**: Your clinical notes stay within your 
#   Azure tenant — they are NOT sent to public OpenAI servers.
#   Azure OpenAI has BAA (Business Associate Agreement) support 
#   for HIPAA-covered entities.
#
# The test call below verifies the connection works before 
# processing any clinical data.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 5 — CODE: Initialize Client and Test Connection          ║
# ╚════════════════════════════════════════════════════════════════╝

from openai import AzureOpenAI

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

# Quick connectivity test — should return "Connection successful"
response = client.chat.completions.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    messages=[{"role": "user", "content": "Say 'Connection successful' if you can read this."}],
    max_tokens=10
)

print(response.choices[0].message.content)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 6 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Loading Clinical Notes from the Silver Layer
#
# Our synthetic dataset contains 150 clinical notes across 
# several note types:
# - **Discharge Summaries**: Comprehensive end-of-stay documents
# - **ED Notes**: Emergency department assessments
# - **Progress Notes**: Daily inpatient updates
# - **Consultation Notes**: Specialist opinions
#
# Each note contains unstructured free text that mirrors real 
# clinical documentation — diagnoses buried in narratives, 
# medications mentioned in passing, and follow-up instructions 
# scattered throughout.
#
# ### Why this is hard for traditional software
# Traditional rule-based NLP requires thousands of hand-coded 
# patterns (regex, keyword lists, grammar rules) and still 
# misses context-dependent meanings. LLMs understand medical 
# language naturally because they were trained on vast medical 
# literature.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 7 — CODE: Load Clinical Notes                            ║
# ╚════════════════════════════════════════════════════════════════╝

# For schema-enabled lakehouses, use 3-part name: lakehouse.schema.table
# If your lakehouse does NOT have schema enabled, use just the table name
df_notes = spark.sql("SELECT * FROM HealthcareLakehouse.dbo.silver_clinical_notes")

print(f"Total clinical notes: {df_notes.count()}")
df_notes.groupBy("note_type").count().show()

# Preview a sample note — notice the unstructured free text
sample_note = df_notes.filter("note_type = 'Discharge Summary'").first()
print(f"\n--- Sample Discharge Summary ---")
print(f"Patient: {sample_note['patient_id']}")
print(f"Date: {sample_note['note_date']}")
print(f"Note:\n{sample_note['note_text'][:800]}...")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 8 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Task 1: Clinical Note Summarization
#
# ### The Problem
# A typical discharge summary is 500-2,000 words. A hospitalist 
# covering 20 patients has to read 20+ notes per shift. A concise 
# 2-3 sentence summary enables rapid triage of what happened.
#
# ### Prompt Engineering Strategy
# Our system prompt tells the LLM to act as a **clinical 
# documentation specialist** and produce:
# 1. Primary condition and presentation
# 2. Key findings or interventions  
# 3. Disposition or next steps
#
# ### Temperature Setting: 0.3 (Low)
# - **Temperature** controls randomness in text generation
# - `0.0` = completely deterministic (always same answer)
# - `1.0` = highly creative (great for stories, bad for facts)
# - `0.3` = mostly consistent but allows minor phrasing variation
# - For clinical tasks, LOW temperature is critical — we want 
#   facts, not creativity


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 9 — CODE: Clinical Note Summarization                    ║
# ╚════════════════════════════════════════════════════════════════╝

def summarize_clinical_note(note_text, note_type):
    """Use Azure OpenAI to create a concise summary of a clinical note.
    
    The prompt is structured to extract the most clinically relevant 
    information in a consistent format that downstream systems can consume.
    """
    
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

# Test with a sample note — compare input vs output
sample = df_notes.filter("note_type = 'Discharge Summary'").first()

print("📋 ORIGINAL NOTE (may be 500+ words):")
print(sample["note_text"][:600])
print("\n" + "=" * 60)

summary = summarize_clinical_note(sample["note_text"], sample["note_type"])
print("\n🤖 AI SUMMARY (2-3 sentences):")
print(summary)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 10 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Task 2: Medical Entity Extraction (Named Entity Recognition)
#
# ### What is entity extraction?
# Entity extraction (a form of NER — Named Entity Recognition) 
# identifies and categorizes specific items from unstructured text:
# - **Diagnoses**: "Type 2 diabetes mellitus", "acute CHF exacerbation"
# - **Medications**: "metformin 500mg BID", "lisinopril 10mg daily"
# - **Procedures**: "chest X-ray", "CT angiography", "central line placement"
# - **Vitals**: "BP 142/88", "SpO2 94%"
# - **Allergies**: "NKDA", "penicillin allergy"
#
# ### Why structured JSON output?
# We instruct the LLM to return **valid JSON** rather than free text. 
# This enables:
# - Direct storage in structured Delta tables
# - Downstream analytics (medication frequency, diagnosis patterns)
# - Integration with clinical decision support systems
#
# ### Temperature: 0.1 (Very Low)
# Entity extraction needs maximum consistency — the same note 
# should produce the same entities every time. We use 0.1 to 
# minimize variation.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 11 — CODE: Medical Entity Extraction                     ║
# ╚════════════════════════════════════════════════════════════════╝
import json

def extract_medical_entities(note_text):
    """Extract structured medical entities from a clinical note.
    
    Uses a JSON schema in the prompt to ensure the LLM returns 
    machine-parseable output. The schema covers the 6 most 
    clinically important entity categories.
    """
    
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
        # Remove markdown code fences if present (LLMs sometimes wrap JSON)
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[-1].rsplit("```", 1)[0]
        return json.loads(result_text)
    except json.JSONDecodeError:
        return {"error": "Failed to parse response as JSON", "raw": result_text}
    except Exception as e:
        return {"error": str(e)}

# Test entity extraction — see how the LLM parses unstructured 
# clinical text into structured categories
sample = df_notes.filter("note_type = 'ED Note'").first()

print("📋 CLINICAL NOTE:")
print(sample["note_text"][:500])
print("\n" + "=" * 60)

entities = extract_medical_entities(sample["note_text"])
print("\n🏥 EXTRACTED ENTITIES:")
print(json.dumps(entities, indent=2))


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 12 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Task 3: ICD-10 Code Suggestion
#
# ### What is ICD-10?
# ICD-10-CM (International Classification of Diseases, 10th 
# Revision, Clinical Modification) is the coding system used for:
# - **Billing**: Every claim submitted to insurance requires ICD-10 codes
# - **Public health**: CDC tracks disease prevalence via ICD-10
# - **Quality reporting**: CMS quality measures reference ICD-10
# - **Research**: Clinical trials use ICD-10 for cohort selection
#
# ### The Coding Problem
# - There are **~72,000 ICD-10-CM codes** — no human memorizes them all
# - Coders must read clinical notes and assign the most specific 
#   applicable codes (e.g., E11.65 = "Type 2 diabetes with 
#   hyperglycemia" not just E11 = "Type 2 diabetes")
# - **Upcoding** (too aggressive) → fraud risk and audits
# - **Undercoding** (too conservative) → lost revenue
# - AI can suggest codes with **evidence** from the note text, 
#   helping coders work faster and more accurately
#
# ### Confidence Scoring
# We ask the model to rate each suggestion as high/medium/low 
# confidence and cite the supporting evidence. Coders can focus 
# review on low-confidence suggestions.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 13 — CODE: ICD-10 Code Suggestion                       ║
# ╚════════════════════════════════════════════════════════════════╝

def suggest_icd10_codes(note_text):
    """Suggest ICD-10 codes based on the clinical note content.
    
    The model is instructed to:
    1. Identify diagnosable conditions in the text
    2. Map each to the most specific ICD-10-CM code
    3. Rate confidence (high/medium/low)
    4. Cite the evidence from the note
    
    This mimics how a certified medical coder works, but in seconds 
    instead of 5-8 minutes per note.
    """
    
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

# Test ICD-10 suggestion — notice how it maps narrative text 
# to specific codes with evidence citations
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


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 14 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Batch Processing: Scaling AI Across All Notes
#
# ### The batch processing pattern
# 
# Real-world clinical AI pipelines process thousands of notes. 
# Here we demonstrate the pattern with a subset of 20 notes:
#
# ```
# For each clinical note:
#   1. Summarize → 2-3 sentence summary
#   2. Extract entities → structured JSON
#   3. Suggest ICD-10 → codes with confidence
#   4. Combine into one record
#   5. Store in Gold layer
# ```
#
# ### Rate limiting considerations
# - Azure OpenAI has rate limits (tokens-per-minute, requests-per-minute)
# - We add a 0.5s delay between notes to stay within limits
# - Production systems use async processing, queues, and retry logic
# - For large volumes (10,000+ notes), use Azure OpenAI batch API
#
# ### Cost awareness
# - Each note triggers 3 API calls ≈ 2,000-3,000 tokens total
# - At GPT-4o-mini pricing (~$0.15/M input tokens): 
#   20 notes ≈ $0.01, 10,000 notes ≈ $5-10


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 15 — CODE: Batch Process Clinical Notes                  ║
# ╚════════════════════════════════════════════════════════════════╝
import time

# Collect notes to process (limit to 20 for lab time constraints)
notes_pdf = df_notes.limit(20).toPandas()

results = []

print(f"Processing {len(notes_pdf)} clinical notes...\n")

for idx, row in notes_pdf.iterrows():
    note_id = row["note_id"]
    patient_id = row["patient_id"]
    note_type = row["note_type"]
    note_text = row["note_text"]
    
    print(f"[{idx+1}/{len(notes_pdf)}] Processing {note_id} ({note_type})...", end=" ")
    
    # Run all 3 AI tasks for each note
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
    
    # Delay to respect rate limits
    time.sleep(0.5)

print(f"\n✅ Processed {len(results)} notes!")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 16 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Saving AI Insights to the Gold Layer
#
# The AI-generated insights are saved as a Delta table called 
# `gold_clinical_ai_insights`. This table contains:
#
# | Column | Type | Description |
# |--------|------|-------------|
# | note_id | string | Links back to silver_clinical_notes |
# | patient_id | string | Links to silver_patients |
# | note_type | string | Discharge Summary, ED Note, etc. |
# | ai_summary | string | 2-3 sentence AI summary |
# | extracted_diagnoses | string (JSON array) | Parsed diagnoses |
# | extracted_medications | string (JSON array) | Parsed medications |
# | extracted_procedures | string (JSON array) | Parsed procedures |
# | suggested_icd10_codes | string (JSON array) | Suggested codes |
# | suggested_icd10_details | string (JSON array) | Codes + evidence |
#
# This table can be:
# - Joined with patient/encounter tables for population-level analysis
# - Surfaced in Power BI dashboards for coding review workflows
# - Used by the Data Agent to answer questions about AI findings


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 17 — CODE: Save AI Insights to Gold Layer               ║
# ╚════════════════════════════════════════════════════════════════╝
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


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 18 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Exploring AI-Generated Insights
#
# Now that the AI has processed the notes, let's examine the 
# results. Key things to look for:
#
# 1. **Summary quality**: Are the summaries concise and accurate?
# 2. **Entity completeness**: Did the AI catch all diagnoses and meds?
# 3. **Code plausibility**: Do the ICD-10 codes match the note content?
# 4. **Confidence distribution**: How often does the model say "high" 
#    vs "low" confidence?
#
# In a real deployment, a clinical informatics team would:
# - Compare AI-suggested codes against human coder assignments
# - Calculate precision/recall for entity extraction
# - Track summary accuracy ratings from clinician reviewers
# - Monitor for bias or hallucination patterns


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 19 — CODE: Explore AI-Generated Insights                ║
# ╚════════════════════════════════════════════════════════════════╝

df_insights = spark.sql("SELECT * FROM HealthcareLakehouse.dbo.gold_clinical_ai_insights")

# Display summaries — check: are they concise? accurate? useful?
print("=" * 60)
print("📋 AI-GENERATED CLINICAL SUMMARIES")
print("=" * 60)

for row in df_insights.limit(5).collect():
    print(f"\n🏷  Note: {row['note_id']} | Type: {row['note_type']} | Patient: {row['patient_id']}")
    print(f"   Summary: {row['ai_summary']}")
    print(f"   ICD-10 Codes: {row['suggested_icd10_codes']}")
    print("-" * 60)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 20 — CODE: ICD-10 Code Analysis                         ║
# ╚════════════════════════════════════════════════════════════════╝
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import ArrayType

# Parse the ICD-10 codes array and analyze distribution
# This shows which conditions the AI most frequently identifies
# across the note corpus — useful for population health trending
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
print("   (Higher frequency = more prevalent in our patient population)")
df_codes.groupBy("icd10_code").count().orderBy("count", ascending=False).show(15)

print("\n✅ Gen AI Clinical Intelligence pipeline complete!")
print("   - Summaries generated and stored")
print("   - Entities extracted to structured JSON")
print("   - ICD-10 codes suggested with confidence scores")
print("   - All results saved in gold_clinical_ai_insights")
