---
name: abc-validator
description: Independently validate the Audit / Balance / Control (ABC) records of an SAP M2C landing batch in PostgreSQL against Azure Blob Storage. Use after every ingestion batch, before downstream loads consume a batch, or when audit asks to prove a load was complete and accurate.
version: 1.0.0
classification: Restricted - Financial systems
owner: EDP Data Engineering
---

# ABC framework validation

`m2c-ingest validate-abc` re-derives every claim the pipeline wrote and stores the outcome in
`abc.validation_run` / `abc.validation_result`.

```bash
m2c-ingest --config tools-registry/config/sap_m2c_pipeline.yaml validate-abc \
    [--batch-id N] [--verify-mode metadata|full] [--skip-storage]
```

| Rule | Layer | What it proves |
| --- | --- | --- |
| A01-A04 | Audit | Batch closed; every file terminal; fingerprints present; batch counters = file rows |
| B01-B05 | Balance | Rows = SAP trailer; blob size = source; no failed check on a reconciled file; mandatory target checks ran; control-total variance within tolerance |
| C01-C05 | Control | Every rejection explained; quarantine never in landing; no double ingestion; duplicates point to an earlier load; batch status matches file outcomes |
| S01-S03 | Storage | Reconciled blobs still exist; stored bytes match the audited md5/sha256 (`full` re-downloads); no unaudited blobs under the batch path |

## Rules for Claude

- `overall_status = FAIL` blocks downstream consumption of that batch. Report the rule ids and
  samples from the JSON output; do not edit `abc.*` rows to make a rule pass.
- Use `--verify-mode full` for month-end close batches or when tampering is suspected; it reads every byte back.
- `--skip-storage` is for database-only triage when storage is unreachable; say so in the report.
