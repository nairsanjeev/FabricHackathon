# Module 11 (Optional): Combine Unstructured + Structured Data with Azure AI Search and Fabric Data Agent

| Duration | 45-60 minutes |
|----------|---------------|
| Objective | Connect an Azure AI Search index (unstructured documents) to a Fabric Data Agent that also uses your semantic model/Lakehouse tables, then validate combined answers with citations and metrics |
| Fabric Features | Data Agent, Azure AI Search Index connector (preview), Lakehouse/Semantic model data sources |
| Prerequisites | Module 3 and Module 7 completed |

---

## Why This Lab?

Many healthcare questions require both:

- **Structured data**: readmission rates, LOS, denial rates, ED utilization
- **Unstructured data**: discharge protocols, care pathways, policy documents, medication guidance

This lab shows how to produce a single answer that combines both worlds. Example:

> "Our 30-day readmission rate for CHF is 17.8% at Metro General, which is above target. The discharge protocol recommends follow-up within 7 days and medication reconciliation before discharge."

---

## Scenario

You are a quality improvement analyst at HealthFirst. You need answers that combine:

1. **Performance metrics** from Fabric data sources
2. **Clinical policy context** from indexed documents in Azure AI Search

You will configure one Data Agent to use both source types and return a combined response.

---

## Microsoft Learn Reference

Primary reference used for this lab:

- [Connect Data Agents to your Azure Search Index in Microsoft Foundry](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-ai-search-index)

> Note: This feature is in preview and may vary slightly by tenant.

---

## What You Will Do

1. Create or identify an Azure AI Search index for healthcare documents
2. Configure RBAC and collect the Azure AI Search resource URL
3. Connect the index in your Fabric Data Agent
4. Keep/add structured Fabric sources (Lakehouse or semantic model)
5. Add routing/instruction prompts so the agent combines both sources
6. Test real healthcare use cases and validate combined output quality

---

## Part A: Set Up Azure AI Search (Unstructured Source)

### Step 1: Prepare the Sample Documents in This Repo

This repo now includes ready-to-index sample files at:

- `data/ai_search_sample_docs/`

For this lab, index the Markdown documents directly from Blob Storage:

- `chf_discharge_protocol.md`
- `sepsis_escalation_workflow.md`
- `ed_triage_policy.md`
- `denial_prevention_checklist.md`
- `care_coordination_sop.md`
- `medication_reconciliation_policy.md`

Optional advanced path:


### Step 2: Check Prerequisites (Portal + Access)

Use the official quickstart flow:

- [Quickstart: Full-text search in the Azure portal](https://learn.microsoft.com/en-us/azure/search/search-get-started-portal?pivots=import-data-new)

Before running the wizard, verify:

1. Azure AI Search service exists (Basic tier or higher recommended for managed identity scenarios).
2. Azure Storage account exists (Blob Storage in same region is preferred).
3. Public network access is enabled temporarily for wizard setup.
4. Search service has room for new objects (`Index`, `Indexer`, `Data Source`) if using Free tier limits.

### Step 3: Configure RBAC (Detailed)

On your Azure AI Search service:

1. Enable role-based access control for data plane.
2. Ensure your user has:
   - `Search Service Contributor`
   - `Search Index Data Contributor`
   - `Search Index Data Reader`
3. If you use managed identity between Search and Storage, enable system-assigned identity on the Search service.

On your Storage account:

1. Assign `Storage Blob Data Owner` to the user performing upload/setup in the portal.
2. Assign `Storage Blob Data Reader` to the Search service managed identity.
3. Confirm role assignments are at account or container scope that includes your upload container.

### Step 4: Upload Sample Markdown Files to Blob Storage

1. Open your Storage account in Azure portal.
2. Go to **Data storage** -> **Containers**.
3. Create a container, for example: `healthcare-search-data`.
4. Upload the six `.md` files from `data/ai_search_sample_docs/`.

### Step 5: Start Import Wizard (Import Data)

1. Open your Azure AI Search service.
2. Select **Import data**.
3. Choose data source: **Azure Blob Storage**.
4. Choose the mode aligned to the quickstart pivot `import-data-new`.

### Step 6: Connect to Data Source in Wizard

1. Select your subscription and storage account.
2. Select container `healthcare-search-data`.
3. Parsing mode: **Default document parsing** for Markdown/blob files.
4. Authentication: **Authenticate using managed identity** (or key-based auth if RBAC is not available).
5. Continue to next step.

### Step 7: AI Enrichment Step

For a fast MVP index, skip enrichment initially and continue.

You can add enrichment later for:

- Entity extraction
- Key phrase extraction
- Chunking/embedding workflows

### Step 8: Configure the Index Schema

When indexing Markdown files from Blob, keep these fields available in the index:

| Field | Type | Attributes |
|---|---|---|
| `metadata_storage_path` | `Edm.String` | Retrievable, Filterable |
| `metadata_storage_name` | `Edm.String` | Searchable, Retrievable, Filterable |
| `content` | `Edm.String` | Searchable, Retrievable |
| `sourceUrl` | `Edm.String` | Retrievable, Filterable |
| `filePath` | `Edm.String` | Retrievable, Filterable |

Recommended mapping for citations:

1. Map `sourceUrl` to the blob URL or another valid document link.
2. Map `filePath` to `metadata_storage_path` (or a custom path field).
3. Keep `content` searchable.

If your wizard-generated schema doesn't include `sourceUrl` and `filePath`, you can still proceed for retrieval, but Data Agent citations might not appear until at least one required citation field name is present.

Important for Fabric Data Agent citations:

- Keep one case-sensitive citation field name exactly as one of:
  - `url`
  - `sourceUrl`
  - `filePath`
  - `path`
  - `folderPath`

For Data Agent citations, include at least one required field name such as `sourceUrl` or `filePath`.

### Step 9: Advanced Settings and Object Names

1. If asked for semantic settings/schedules, you can keep defaults for first run.
2. Set object prefix clearly, for example: `healthcare-policies`.
3. Create objects (Data Source, Index, Indexer) and run indexer.

### Step 10: Validate Index Build

1. Open **Search management** -> **Indexers** and wait for status `Success`.
2. Open **Search management** -> **Indexes** -> your index.
3. Confirm document count > 0.
4. In **Search explorer**, run a query like:
   - `chf discharge follow-up`
   - `sepsis escalation`
5. Confirm results return fields such as `content`, `metadata_storage_path`, and file name/path metadata.

### Step 11: Copy the Search Resource URL

From your Azure AI Search service overview, copy the resource URL (for example `https://<service-name>.search.windows.net`).

You will use this URL in Fabric Data Agent when adding **AI Search Index**.

---

## Part B: Connect AI Search Index to Fabric Data Agent

### Step 12: Open Your Existing Data Agent

1. In your Fabric workspace, open the Data Agent from Module 7 (or create a new one)
2. Go to the **Data** tab

### Step 13: Add AI Search Index

1. Select **Add AI Search Index**
2. Paste the Azure AI Search resource URL
3. Confirm connection

### Step 14: Keep/Add Structured Sources

In the same Data Agent, also include your structured sources:

- `gold_readmissions`
- `gold_encounter_summary`
- `gold_facility_summary` (if created)
- `gold_financial`
- `gold_population_health`
- Semantic model used in Module 3 (if available in your tenant flow)

The key outcome is one agent with both source types configured.

---

## Part C: Configure for Combined Responses

### Step 15: Add Data Source Context for AI Search

In the AI Search index data source context/description, add guidance similar to:

```text
This index contains healthcare policy and protocol documents, including discharge guidance,
sepsis response workflow, ED triage procedures, and denial prevention checklists.

Use this source when users ask "what should we do" or request policy/process context.
Prefer structured Fabric sources for numeric KPIs and trend calculations.
```

### Step 16: Configure Index Retrieval Settings

Recommended starting values:

- **Search Type**: Hybrid or Semantic (depending on index capabilities)
- **Number of Documents**: 5-8 (within Microsoft guidance range of 3-20)
- **Display Name**: `Healthcare Policies Index`

Tune this later for precision vs recall.

### Step 17: Update Agent Instructions for Source Blending

Use instructions like this in your Data Agent:

```text
You are a healthcare analytics assistant for HealthFirst.

When a user asks a question:
1) Use structured Fabric sources (Lakehouse/semantic model) for quantitative answers
   such as rates, counts, trends, and facility comparisons.
2) Use Azure AI Search index for policy, protocol, and unstructured clinical guidance.
3) If both are relevant, produce a combined response with:
   - Metrics section (with values and facility scope)
   - Guidance section (policy/protocol summary)
   - Citations section (document paths/URLs)
4) Never fabricate citations. If a document is not retrieved, state that explicitly.
5) If data is insufficient in either source, clearly call out the gap.
```

---

## Part D: Validate with Healthcare Use Cases

### Step 18: Test Combined Prompts

Run these prompts in the Data Agent:

1. `What is our CHF 30-day readmission rate by facility, and what discharge protocol should be followed to reduce readmissions?`
2. `Which facility has the highest ED frequent-flyer volume, and what triage policy guidance applies?`
3. `What is our claim denial rate for Medicare, and what documentation checklist can reduce denials?`
4. `Summarize sepsis-related inpatient trends and include recommended escalation steps from policy documents.`

### Step 19: Confirm the Response Structure

For each answer, verify:

- It includes structured metrics (numbers, percentages, counts)
- It includes unstructured policy/protocol guidance
- Citations are present for document-derived statements
- Facility scope/time scope is explicit
- No unsupported claims are presented as facts

---

## Part E: Troubleshooting

If unstructured content is not used:

- Confirm index connection is healthy in Data Agent
- Lower/increase `Number of Documents` (for recall/precision tuning)
- Improve AI Search context description (make scope explicit)
- Confirm documents contain searchable content in the target fields

If citations are missing:

- Ensure one of these fields exists exactly: `url`, `sourceUrl`, `filePath`, `path`, `folderPath`
- Confirm retrieved chunks include those fields

If answers are metric-only or text-only:

- Refine instructions to explicitly require a two-part response
- Add a line: "When both source types are relevant, always provide both"
- Add 2-3 representative example prompts in the agent configuration

---

## Success Criteria

You are done when your Data Agent can answer at least two prompts with:

1. Correct structured KPI output from Fabric data
2. Relevant policy/protocol context from Azure AI Search
3. Visible citations to supporting documents
4. A single integrated answer suitable for a clinical operations meeting

---

## Extension Ideas

- Add a `document_confidence` field and ask the agent to report evidence quality
- Segment policy docs by facility and test site-specific grounding
- Build an evaluation notebook (similar to Module 7B) that scores:
  - metric correctness
  - citation presence
  - guidance relevance
