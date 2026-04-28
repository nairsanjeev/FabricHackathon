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
# ⚠️ SESSION NOTE:
#   If your Spark session expires or is stopped, you will need
#   to re-run all cells from the top. Fabric does not preserve
#   variables, imports, or DataFrames across session restarts.
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
# HOW TO GET THESE VALUES FROM AI FOUNDRY:
#   1. Go to https://ai.azure.com → sign in → open your project
#   2. ENDPOINT & KEY:
#      - Click "Management center" (gear icon) → "Connected resources"
#      - Click your Azure OpenAI connection → copy Endpoint and Key
#      - OR: Azure Portal → your OpenAI resource → "Keys and Endpoint"
#   3. DEPLOYMENT NAME:
#      - In AI Foundry → "Models + endpoints" → "Deployments" tab
#      - Copy the deployment name (e.g., "gpt-4o-mini", "gpt-4.1")
#      - If no deployment exists: click "+ Deploy model" → select a GPT model → deploy
#
# IMPORTANT: The endpoint must be ONLY the base URL — do NOT include
# any path like /openai/v1 or /openai/deployments/...
# The SDK appends the correct path automatically.
#
# ✅ Correct:   https://my-resource.openai.azure.com/
# ❌ Wrong:     https://my-resource.openai.azure.com/openai/v1
# ❌ Wrong:     https://my-resource.openai.azure.com/openai/deployments/gpt-4o

AZURE_OPENAI_ENDPOINT = "https://<your-resource-name>.openai.azure.com/"  # From AI Foundry
AZURE_OPENAI_KEY = "<your-api-key>"                                        # From AI Foundry
AZURE_OPENAI_DEPLOYMENT = "<your-deployment-name>"                          # From AI Foundry
AZURE_OPENAI_API_VERSION = "2024-06-01"

print("✅ Configuration set!")
print(f"   Endpoint: {AZURE_OPENAI_ENDPOINT[:40]}...")
print(f"   Deployment: {AZURE_OPENAI_DEPLOYMENT}")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — CODE: Install and Initialize OpenAI SDK             ║
# ╚════════════════════════════════════════════════════════════════╝

%pip install openai -q
# ⚠️ EXPECTED WARNINGS (safe to ignore):
#   - "ERROR: pip's dependency resolver..." — a pre-installed Fabric package (nni)
#     has a stale constraint. It does NOT affect the openai package or this lab.
#   - "A new release of pip is available" — informational only.
#   - "PySpark kernel has been restarted" — expected. Wait for the restart, then
#     continue with the next cell.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Initializing the Azure OpenAI Client
#
# ### 1. What this step does (business meaning)
# 
# We create a connection to YOUR Azure-hosted GPT model. This is 
# different from the public OpenAI API — Azure OpenAI runs within 
# your Azure tenant, which means:
# - ✅ **Data residency:** Clinical notes stay in YOUR Azure region 
#   and are NOT sent to public OpenAI servers
# - ✅ **HIPAA compliance:** Azure OpenAI supports BAA (Business 
#   Associate Agreement) for covered entities
# - ✅ **Network isolation:** Can be deployed in a private VNet
# - ✅ **Enterprise SLA:** 99.9% uptime guarantee
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Import the SDK
#     from openai import AzureOpenAI
# - The `openai` Python package supports both public OpenAI and 
#   Azure OpenAI. `AzureOpenAI` is the Azure-specific client class.
# - The SDK handles authentication, retry logic, and endpoint 
#   routing automatically.
#
# #### Step 2: Create the client
#     client = AzureOpenAI(
#         azure_endpoint=AZURE_OPENAI_ENDPOINT,
#         api_key=AZURE_OPENAI_KEY,
#         api_version=AZURE_OPENAI_API_VERSION)
# - `azure_endpoint`: Your resource's base URL 
#   (e.g., `https://my-resource.openai.azure.com/`). The SDK 
#   appends `/openai/deployments/...` automatically.
# - `api_key`: 32-character key from the Azure portal. In 
#   production, you'd use Azure Key Vault or Managed Identity.
# - `api_version`: `"2024-06-01"` is the stable GA version with 
#   full chat completion support.
#
# #### Step 3: Test the connection
#     response = client.chat.completions.create(
#         model=AZURE_OPENAI_DEPLOYMENT,
#         messages=[{"role": "user", "content": "Say 'Connection successful'"}],
#         max_tokens=10)
# - Sends a minimal test message to verify the endpoint, key, 
#   and deployment name are all correct
# - `model=` is actually the DEPLOYMENT name (not the model name) 
#   in Azure OpenAI
# - If this fails, check: endpoint URL format, API key, deployment 
#   existence, and network access
#
# ### 3. Summary
#
# The AzureOpenAI client is initialized with your endpoint, API key, 
# and API version. A test call verifies connectivity before processing 
# any clinical data. All communication stays within your Azure tenant.


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
# ### 1. What this step does (business meaning)
#
# We load 150 synthetic clinical notes that mirror real hospital 
# documentation. These notes are the INPUT to our AI pipeline.
#
# Note types in our dataset:
# - **Discharge Summaries** — Comprehensive end-of-stay documents 
#   (longest, most detailed, highest coding value)
# - **ED Notes** — Emergency department assessments (time-critical, 
#   often abbreviated)
# - **Progress Notes** — Daily inpatient updates (brief, incremental)
# - **Consultation Notes** — Specialist opinions (domain-specific 
#   terminology)
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Load from schema-enabled Lakehouse
#     df_notes = spark.sql("SELECT * FROM HealthcareLakehouse.dbo.silver_clinical_notes")
# - Uses **3-part naming** (`lakehouse.schema.table`) required for 
#   schema-enabled Lakehouses in Microsoft Fabric
# - If your Lakehouse does NOT have schema enabled, use just the 
#   table name: `spark.table("silver_clinical_notes")`
#
# #### Step 2: Explore the data
#     df_notes.groupBy("note_type").count().show()
# - Validates all note types are present and shows volume distribution
# - A preview of one sample note shows the raw unstructured text that 
#   the AI will process
#
# ### 3. Why this is hard for traditional software
# Traditional rule-based NLP requires thousands of hand-coded patterns 
# (regex, keyword lists, grammar rules) and misses context-dependent 
# meanings. LLMs understand medical language naturally because they 
# were trained on vast medical literature.


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
# ### 1. What this task does (business meaning)
#
# A typical discharge summary is 500–2,000 words. A hospitalist 
# covering 20 patients has to read 20+ notes per shift to understand 
# what happened to each patient. AI summarization reduces each note 
# to a concise 2–3 sentence summary, enabling rapid triage.
#
# **Time savings:** 5–10 minutes per manual read → 3–5 seconds with AI
#
# ### 2. Step-by-step walkthrough of the function
#
# #### Step 1: System prompt design
#     system_prompt = """You are a clinical documentation specialist.
#     Summarize the following clinical note into a concise 2-3 sentence
#     summary that captures: 1. Primary condition and presentation,
#     2. Key findings or interventions, 3. Disposition or next steps"""
# - The system prompt defines the AI's **persona** and **task structure**
# - Specifying "2–3 sentences" controls output length
# - The three-part structure ensures clinically relevant content is 
#   consistently captured across all notes
# - "Use medical terminology appropriately" tells the model to write 
#   for a clinical audience, not a lay audience
#
# #### Step 2: Message structure
#     messages=[
#         {"role": "system", "content": system_prompt},
#         {"role": "user", "content": f"Note Type: {note_type}\n\n..."}]
# - The chat completion API uses a message array with roles:
#   - ✅ `system` → Sets behavior, persona, and task instructions
#   - ✅ `user` → Provides the actual input to process
# - Including `note_type` in the user message gives context — a 
#   Discharge Summary needs a different summary style than an ED Note
#
# #### Step 3: Parameters
#     max_tokens=200, temperature=0.3
# - `max_tokens=200` → Hard cap on output length (~150 words). 
#   Prevents runaway generation.
# - `temperature=0.3` → **Low randomness**. Temperature controls 
#   "creativity" on a 0–1 scale:
#   - ✅ 0.0 = Completely deterministic (always same output)
#   - ✅ 0.3 = Mostly consistent with minor variation (our choice)
#   - ✅ 1.0 = Maximum creativity (great for stories, bad for clinical)
# - For medical summarization, we want FACTS, not creativity.
#
# #### Step 4: Error handling
#     except Exception as e:
#         return f"Error: {str(e)}"
# - API calls can fail (network timeout, rate limit, invalid key). 
#   The try/except ensures one bad note doesn't crash the entire 
#   batch processing pipeline.
#
# ### 3. Summary
#
# The summarize function sends each clinical note to Azure OpenAI 
# with a system prompt that requests a structured 2–3 sentence summary 
# covering condition, interventions, and disposition. Low temperature 
# (0.3) ensures factual consistency across runs.


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
# ### 1. What this task does (business meaning)
#
# Entity extraction (NER — Named Entity Recognition) identifies and 
# categorizes specific clinical items from unstructured text into 
# structured, machine-readable data:
# - **Diagnoses:** "Type 2 diabetes mellitus", "acute CHF exacerbation"
# - **Medications:** "metformin 500mg BID", "lisinopril 10mg daily"
# - **Procedures:** "chest X-ray", "CT angiography", "central line"
# - **Vitals:** "BP 142/88", "SpO2 94%"
# - **Allergies:** "NKDA", "penicillin allergy"
#
# ### 2. Step-by-step walkthrough of the function
#
# #### Step 1: System prompt with JSON schema
#     system_prompt = """You are a clinical NLP system. Extract...
#     {
#         "diagnoses": ["list of diagnoses"],
#         "medications": ["list of medications"],
#         "procedures": ["list of procedures"],
#         "vitals_mentioned": ["any vital signs with values"],
#         "allergies": ["any allergies"],
#         "follow_up": "follow-up instructions"
#     }"""
# - The prompt INCLUDES the exact JSON schema the model should return
# - This is a form of **structured output** prompting — telling the 
#   LLM what format to use ensures machine-parseable results
# - Six entity categories cover the most clinically important 
#   information types
# - "Only include entities that are explicitly mentioned" prevents 
#   the model from hallucinating entities
#
# #### Step 2: Very low temperature
#     temperature=0.1
# - Entity extraction needs MAXIMUM consistency — the same note 
#   should produce the same entities every time
# - Temperature 0.1 is nearly deterministic while still allowing 
#   minor phrasing flexibility
#
# #### Step 3: Parse the JSON response
#     result_text = response.choices[0].message.content.strip()
#     if result_text.startswith("```"):
#         result_text = result_text.split("\n", 1)[-1].rsplit("```", 1)[0]
#     return json.loads(result_text)
# - LLMs sometimes wrap JSON in markdown code fences 
#   (` ```json ... ``` `). We strip these before parsing.
# - `json.loads()` converts the string to a Python dict
# - If parsing fails, the `except json.JSONDecodeError` handler 
#   returns the raw text so we can debug the model's output
#
# ### 3. Summary
#
# The entity extraction function sends clinical notes to Azure 
# OpenAI with a JSON schema prompt that defines six entity 
# categories (diagnoses, medications, procedures, vitals, allergies, 
# follow-up). Temperature 0.1 ensures consistent extraction, and 
# the response is parsed from JSON into a Python dict for 
# downstream processing.


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
# ### 1. What this task does (business meaning)
#
# ICD-10-CM is the universal coding system for clinical diagnoses. 
# There are **~72,000 codes** — no human memorizes them all. Medical 
# coders read clinical notes and assign codes manually, a process that 
# takes 5–8 minutes per note and is error-prone.
#
# Two types of coding errors:
# - **Upcoding** (too aggressive) → Fraud risk, CMS audits, penalties
# - **Undercoding** (too conservative) → Lost revenue, inaccurate 
#   risk adjustment, understated severity
#
# AI code suggestion helps coders work faster and more accurately by 
# proposing codes with evidence citations from the note text.
#
# ### 2. Step-by-step walkthrough of the function
#
# #### Step 1: System prompt as a certified coder
#     system_prompt = """You are a certified medical coder (CPC).
#     Based on the clinical note, suggest ICD-10-CM codes.
#     Return a JSON array: [{"code": "...", "description": "...",
#     "confidence": "high/medium/low", "evidence": "..."}]"""
# - The persona "certified medical coder (CPC)" primes the model 
#   to think like a professional coder
# - The JSON schema requires four fields per suggestion:
#   - ✅ `code` → The ICD-10-CM code (e.g., "E11.65")
#   - ✅ `description` → Official code description
#   - ✅ `confidence` → high/medium/low self-assessment
#   - ✅ `evidence` → Text from the note supporting this code
# - Evidence citations enable coders to **verify** each suggestion 
#   against the source text, supporting a human-in-the-loop workflow
#
# #### Step 2: Temperature 0.2
#     temperature=0.2
# - Slightly higher than entity extraction (0.1) because code 
#   selection involves some clinical judgment (choosing between 
#   similar codes), not just extraction
# - Still low enough to ensure consistency across runs
#
# #### Step 3: Max tokens 600
#     max_tokens=600
# - Higher than summarization (200) because each code suggestion 
#   includes 4 fields, and a note may have 5–10 diagnosable conditions
#
# ### 3. Summary
#
# The ICD-10 suggestion function sends clinical notes to Azure OpenAI 
# with a certified-coder persona prompt. It returns a JSON array of 
# code suggestions, each with the ICD-10 code, description, confidence 
# level, and evidence from the note text. This enables a 
# human-in-the-loop coding review workflow.


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
# ### 1. What this step does (business meaning)
# 
# Real-world clinical AI pipelines process thousands of notes per day. 
# This cell demonstrates the batch processing pattern with a subset 
# of 20 notes, running all three AI tasks on each note.
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Collect notes to the driver node
#     notes_pdf = df_notes.limit(20).toPandas()
# - `limit(20)` restricts to 20 notes for the lab (cost/time control)
# - `toPandas()` moves data from Spark executors to the driver node
# - **Why toPandas()?** Azure OpenAI is a REST API that can only be 
#   called from the driver, not from distributed Spark executors.
# - For 10,000+ notes in production, you'd use:
#   - ✅ `mapPartitions()` with API calls inside the function
#   - ✅ Azure OpenAI Batch API (async, 50% cheaper)
#   - ✅ Azure AI Services container for on-cluster inference
#
# #### Step 2: Process each note sequentially
#     for idx, row in notes_pdf.iterrows():
#         summary = summarize_clinical_note(note_text, note_type)
#         entities = extract_medical_entities(note_text)
#         codes = suggest_icd10_codes(note_text)
# - Each note triggers 3 API calls (summarize, extract, code)
# - Sequential processing with progress printing for visibility
#
# #### Step 3: Combine outputs into one record
#     results.append({
#         "note_id": note_id, ...
#         "extracted_diagnoses": json.dumps(entities.get("diagnoses", [])),
#         "suggested_icd10_codes": json.dumps([...])  })
# - All AI outputs for one note are merged into a single dict
# - `json.dumps()` serializes lists/dicts to JSON strings because 
#   Delta tables need flat columns (no nested arrays)
# - `entities.get("diagnoses", [])` safely handles missing keys
#
# #### Step 4: Rate limiting
#     time.sleep(0.5)
# - 0.5-second delay between notes prevents hitting Azure OpenAI 
#   token-per-minute (TPM) or request-per-minute (RPM) limits
# - At 3 calls/note × 2 notes/sec = 6 requests/sec, well within 
#   default limits of 60 RPM
#
# ### 3. Cost awareness
# - Each note: ~2,000–3,000 tokens across 3 calls
# - At GPT-4o-mini pricing (~$0.15/M input tokens):
#   - ✅ 20 notes ≈ $0.01
#   - ✅ 10,000 notes ≈ $5–10
#
# ### 4. Summary
#
# The batch loop collects 20 notes to the driver, runs three AI 
# tasks per note (summarize, extract, code), serializes structured 
# outputs as JSON strings, and adds rate-limiting delays. Results 
# are stored in a Python list for conversion to a Delta table next.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 15 — CODE: Batch Process Clinical Notes                  ║
# ╚════════════════════════════════════════════════════════════════╝
import time

# Collect notes to the driver node as a Pandas DataFrame
# WHY toPandas()? Azure OpenAI is a REST API call — it can only
# be called from the driver node, not from Spark executors.
# For 20 notes, this is fine. For 10,000+ notes, you'd use:
#   - mapPartitions() with the API calls inside the function
#   - Azure OpenAI Batch API (async, cheaper)
#   - Azure AI Services container for on-cluster inference
notes_pdf = df_notes.limit(20).toPandas()

results = []

print(f"Processing {len(notes_pdf)} clinical notes...\n")

for idx, row in notes_pdf.iterrows():
    note_id = row["note_id"]
    patient_id = row["patient_id"]
    note_type = row["note_type"]
    note_text = row["note_text"]
    
    print(f"[{idx+1}/{len(notes_pdf)}] Processing {note_id} ({note_type})...", end=" ")
    
    # Run all 3 AI tasks sequentially for each note
    # Each task is a separate API call with its own system prompt
    summary = summarize_clinical_note(note_text, note_type)
    entities = extract_medical_entities(note_text)
    codes = suggest_icd10_codes(note_text)
    
    # Combine all AI outputs into a single record
    # We JSON-serialize lists/dicts because Delta tables need flat columns
    # (Delta doesn't natively support nested arrays as column types)
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
    
    # Rate limit delay: 0.5s between notes prevents hitting Azure OpenAI
    # token-per-minute (TPM) or request-per-minute (RPM) limits
    # At 3 calls/note × 2 notes/sec = 6 RPM, well within default limits
    time.sleep(0.5)

print(f"\n✅ Processed {len(results)} notes!")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 16 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Saving AI Insights to the Gold Layer
#
# ### 1. What this step does (business meaning)
#
# The AI-generated insights are persisted as a Delta table called 
# `gold_clinical_ai_insights`. This makes the AI outputs queryable 
# by SQL, joinable with patient/encounter tables, and consumable 
# by Power BI dashboards and Data Agents.
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Define an explicit schema
#     schema = StructType([
#         StructField("note_id", StringType(), True),
#         StructField("patient_id", StringType(), True),
#         ...])
# - **Why explicit schema?** Without it, Spark infers types from 
#   the data — which can be wrong if a batch has unusual values. 
#   Explicit schema guarantees:
#   - ✅ Column names match downstream consumers' expectations
#   - ✅ All columns are StringType (JSON arrays stored as strings)
#   - ✅ No surprises from empty or null results
#
# #### Step 2: Convert Python → Spark → Delta
#     df_ai_insights = spark.createDataFrame(results, schema=schema)
#     df_ai_insights.write.mode("overwrite").format("delta").saveAsTable(...)
# - `createDataFrame(results, schema)` converts the Python list of 
#   dicts (driver-side data) into a distributed Spark DataFrame
# - `mode("overwrite")` makes the notebook idempotent (safe to re-run)
# - `saveAsTable()` registers it in the Lakehouse catalog
#
# ### 3. Output column reference
#
# | Column | Type | Description |
# |--------|------|-------------|
# | note_id | string | Links back to silver_clinical_notes |
# | patient_id | string | Links to silver_patients |
# | note_type | string | Discharge Summary, ED Note, etc. |
# | ai_summary | string | 2-3 sentence AI summary |
# | extracted_diagnoses | string (JSON) | Parsed diagnoses array |
# | extracted_medications | string (JSON) | Parsed medications array |
# | extracted_procedures | string (JSON) | Parsed procedures array |
# | suggested_icd10_codes | string (JSON) | Code strings only |
# | suggested_icd10_details | string (JSON) | Codes + evidence + confidence |
#
# ### 4. Summary
#
# The batch results are converted from a Python list to a Spark 
# DataFrame with an explicit schema, then written as a Delta table. 
# This table can be joined with patient data, surfaced in Power BI, 
# or queried by the Data Agent.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 17 — CODE: Save AI Insights to Gold Layer               ║
# ╚════════════════════════════════════════════════════════════════╝
from pyspark.sql.types import StructType, StructField, StringType

# Define explicit schema — WHY not let Spark infer it?
# With explicit schema, we guarantee:
#   1. Column names match exactly what downstream consumers expect
#   2. All columns are StringType (JSON arrays stored as strings)
#   3. No surprises if a batch has empty/null results
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

# Convert Python list of dicts → Spark DataFrame → Delta table
# This is the Pandas → Spark bridge: createDataFrame() distributes
# the driver-side data across the Spark cluster for storage
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
# ### 1. What this step does (business meaning)
#
# Now that the AI pipeline has processed the notes, we examine the 
# results to assess quality. In a real clinical deployment, this is 
# the **validation phase** before any AI output reaches clinicians.
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Load the Gold AI table
#     df_insights = spark.sql("SELECT * FROM HealthcareLakehouse.dbo.gold_clinical_ai_insights")
# - Reads from the Gold table we just created, using 3-part naming 
#   for schema-enabled Lakehouses
#
# #### Step 2: Display summaries for quality review
#     for row in df_insights.limit(5).collect():
#         print(row['ai_summary'])
#         print(row['suggested_icd10_codes'])
# - `.collect()` brings 5 rows to the driver for display
# - Review checklist:
#   - ✅ **Summary quality:** Are summaries concise and accurate?
#   - ✅ **Entity completeness:** Did the AI catch all diagnoses/meds?
#   - ✅ **Code plausibility:** Do ICD-10 codes match note content?
#   - ✅ **Confidence distribution:** How often is confidence "high" 
#     vs "low"?
#
# ### 3. What a real deployment would add
# - Compare AI-suggested codes against human coder assignments 
#   (precision/recall metrics)
# - Track summary accuracy ratings from clinician reviewers
# - Monitor for bias, hallucination patterns, or drift over time
# - A/B test: AI-assisted coders vs. manual coders
#
# ### 4. Summary
#
# The exploration cell loads AI insights from the Gold table and 
# displays summaries, extracted entities, and ICD-10 code suggestions 
# for manual quality review. This represents the validation phase 
# of a clinical AI deployment.


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

# Parse the JSON string column back into an actual array,
# then explode it — this "unpivots" the array so each ICD-10 code
# becomes its own row. This enables groupBy() counting.
#
# Before explode: | note_id | codes                        |
#                 | N001    | ["E11.9", "I10", "E78.5"]    |
#
# After explode:  | note_id | icd10_code |
#                 | N001    | E11.9      |
#                 | N001    | I10        |
#                 | N001    | E78.5      |
df_codes = df_insights.select(
    "note_id",
    "patient_id",
    from_json(col("suggested_icd10_codes"), ArrayType(StringType())).alias("codes")
).select(
    "note_id",
    "patient_id",
    explode("codes").alias("icd10_code")
)

# Code frequency analysis — the most frequently suggested codes
# reveal the dominant conditions in our patient population.
# Compare these to the condition_category counts from Notebook 02
# to validate that the AI is identifying the same top conditions.
print("📊 Most Frequently Suggested ICD-10 Codes:")
print("   (Higher frequency = more prevalent in our patient population)")
df_codes.groupBy("icd10_code").count().orderBy("count", ascending=False).show(15)

print("\n✅ Gen AI Clinical Intelligence pipeline complete!")
print("   - Summaries generated and stored")
print("   - Entities extracted to structured JSON")
print("   - ICD-10 codes suggested with confidence scores")
print("   - All results saved in gold_clinical_ai_insights")
