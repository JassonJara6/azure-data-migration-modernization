# Bronze release checklist

Use [the rollout runbook](bronze-end-to-end-rollout.md) for exact commands and two-run acceptance.

- [ ] Review the Bronze-only PR diff; no Databricks/Silver/Gold/Terraform changes.
- [ ] Verify existing metadata and capture/back up live cursor/audit state; do not reset cursors.
- [ ] Review additive audit migration and any legacy duplicates/incomplete receipts.
- [ ] Obtain approval before deployment, Azure execution/publishing or merge.
- [ ] Deploy preflight, audit schema, registration, attestation, child commit and parent commit in documented order.
- [ ] Apply separate least-privilege ADF/trusted-runtime grants; test denied direct writes/attestation from ADF.
- [ ] Install the trusted Python/ODBC dependencies and bind the existing factory/storage settings securely.
- [ ] Validate All in ADF, including parameterized Stored Procedure calls and FULL null values; reuse existing linked services/SHIR.
- [ ] Keep WATERMARK_CONFIRMATION_BLOCKED and acknowledgement gates active.
- [ ] Approve and execute R1: eleven copied, eleven independently reconciled, one atomic committed parent.
- [ ] Retry R1 confirmation: unchanged watermark bytes/versions and durable replay receipt.
- [ ] Execute R2 against unchanged source: five empty incrementals, six FULL, no cursor movement, eleven new receipts.
- [ ] Verify failure/rollback, stale-version rejection and independent landed-file evidence.
- [ ] Retain evidence and obtain approval for any subsequent replacement of ADF's confirmation block.

Local verification in this task includes real Parquet tests, mocked ADF coordination and isolated SQL Server integration. It does not establish the live deployment or R1/R2 acceptance in Azure.
