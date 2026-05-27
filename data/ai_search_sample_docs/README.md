# AI Search Sample Documents

This folder contains sample unstructured healthcare content for Azure AI Search.

## Files

- `chf_discharge_protocol.md`
- `sepsis_escalation_workflow.md`
- `ed_triage_policy.md`
- `denial_prevention_checklist.md`
- `care_coordination_sop.md`
- `medication_reconciliation_policy.md`
- `healthcare_policies_sample.json` (JSON array for import wizard)

## Fastest path for Azure AI Search

If you use the Azure portal quickstart wizard (`Import data`):

1. Upload `healthcare_policies_sample.json` to a blob container.
2. In wizard parsing mode, select `JSON array`.
3. Map fields as described in Module 11.

This path keeps metadata fields such as `sourceUrl`, `documentType`, and `facility` so Fabric Data Agent can provide grounded answers with citations.
