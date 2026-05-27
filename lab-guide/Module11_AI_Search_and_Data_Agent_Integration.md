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

### Step 1: Create an Azure AI Search Index

Use the Azure AI Search quickstart if you do not already have an index:

- [Azure AI Search quickstart (Portal)](https://learn.microsoft.com/en-us/azure/search/search-get-started-portal?pivots=import-data-new)

For this healthcare scenario, index documents such as:

- CHF discharge instructions
- Sepsis early warning workflow
- ED triage protocol
- Claims denial prevention checklist
- Care coordination SOPs

### Step 2: Include Citation-Friendly Fields in the Index Schema

To ensure Data Agent can show citations, include at least one case-sensitive field name:

- `url`
- `sourceUrl`
- `filePath`
- `path`
- `folderPath`

Suggested minimum fields for your index:

- `id` (key)
- `title`
- `content`
- `sourceUrl` (or one citation-friendly field above)
- `documentType`
- `facility`
- `effectiveDate`

### Step 3: Enable RBAC and Assign Roles

On the Azure AI Search resource:

1. Enable role-based access (Microsoft Entra ID / RBAC)
2. Assign required roles to the identity used for setup/testing:
   - `Search Index Data Contributor`
   - `Search Index Data Reader`

### Step 4: Copy the Resource URL

Copy the Azure AI Search resource URL. You will use this in the Data Agent when adding the index connection.

---

## Part B: Connect AI Search Index to Fabric Data Agent

### Step 5: Open Your Existing Data Agent

1. In your Fabric workspace, open the Data Agent from Module 7 (or create a new one)
2. Go to the **Data** tab

### Step 6: Add AI Search Index

1. Select **Add AI Search Index**
2. Paste the Azure AI Search resource URL
3. Confirm connection

### Step 7: Keep/Add Structured Sources

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

### Step 8: Add Data Source Context for AI Search

In the AI Search index data source context/description, add guidance similar to:

```text
This index contains healthcare policy and protocol documents, including discharge guidance,
sepsis response workflow, ED triage procedures, and denial prevention checklists.

Use this source when users ask "what should we do" or request policy/process context.
Prefer structured Fabric sources for numeric KPIs and trend calculations.
```

### Step 9: Configure Index Retrieval Settings

Recommended starting values:

- **Search Type**: Hybrid or Semantic (depending on index capabilities)
- **Number of Documents**: 5-8 (within Microsoft guidance range of 3-20)
- **Display Name**: `Healthcare Policies Index`

Tune this later for precision vs recall.

### Step 10: Update Agent Instructions for Source Blending

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

### Step 11: Test Combined Prompts

Run these prompts in the Data Agent:

1. `What is our CHF 30-day readmission rate by facility, and what discharge protocol should be followed to reduce readmissions?`
2. `Which facility has the highest ED frequent-flyer volume, and what triage policy guidance applies?`
3. `What is our claim denial rate for Medicare, and what documentation checklist can reduce denials?`
4. `Summarize sepsis-related inpatient trends and include recommended escalation steps from policy documents.`

### Step 12: Confirm the Response Structure

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
