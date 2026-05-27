# AI Search Sample Documents

This folder contains sample unstructured healthcare content for Azure AI Search.

## Files

- `chf_discharge_protocol.md`
- `sepsis_escalation_workflow.md`
- `ed_triage_policy.md`
- `denial_prevention_checklist.md`
- `care_coordination_sop.md`
- `medication_reconciliation_policy.md`

## Fastest path for Azure AI Search

Use the Azure portal quickstart wizard (`Import data`) with the Markdown files:

1. Upload all `.md` files in this folder to a blob container.
2. In wizard parsing mode, use default document parsing.
3. Map citation-friendly fields as described in Module 11.

This path lets Azure AI Search index document text directly from Blob Storage and return document paths for citations in Fabric Data Agent.
