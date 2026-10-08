---
name: sap-m2c-ingestor
description: Land SAP Market-to-Cash (M2C) flat-file extracts (KNA1, VBAK, VBAP, VBRK, BSID) from an on-premise network share into Azure Blob Storage, with every file audited, balanced and controlled in the PostgreSQL ABC framework. Use when asked to ingest, re-run, or troubleshoot an SAP M2C landing batch.
version: 1.0.0
classification: Restricted - Financial systems
owner: EDP Data Engineering
---

# SAP M2C ingestion to Azure Blob

The skill wraps the `m2c-ingest` CLI (`src/m2c_ingest`). It never handles secret values itself:
the CLI resolves them from Key Vault at runtime.

## Before running

1. Confirm the catalog in `tools-registry/config/sap_m2c_pipeline.yaml` lists the entity being loaded.
   New SAP tables are added there (columns, keys, trailer layout, control totals), not in code.
2. Required environment:
   - `M2C_SOURCE_ROOT` - mounted share path (`/mnt/sap_outbound/m2c`) or UNC path (`\\server\share\m2c`).
   - `AZURE_KEYVAULT_URL` - vault holding `abc-postgres-dsn` (and the storage secret if used).
   - `AZURE_STORAGE_ACCOUNT_URL` (managed identity, preferred) **or** `M2C_STORAGE_SECRET_NAME`.
3. Identity: the host's managed identity (or `az login` / `AZURE_CLIENT_ID`+`AZURE_TENANT_ID`+`AZURE_CLIENT_SECRET`)
   needs Key Vault *Secrets User* and *Storage Blob Data Contributor* on the landing account.

## Commands

```bash
m2c-ingest --config tools-registry/config/sap_m2c_pipeline.yaml init-db          # once per release
m2c-ingest --config tools-registry/config/sap_m2c_pipeline.yaml ingest --triggered-by "$USER"
m2c-ingest --config tools-registry/config/sap_m2c_pipeline.yaml validate-abc     # latest batch
```

Exit codes: `0` succeeded or nothing new, `1` failed, `2` partial / validation failed, `3` configuration error.

## Rules for Claude

- Always run `validate-abc` after `ingest` and report its `overall_status` with the batch summary.
- Never re-upload or delete blobs by hand; re-deliver the file to the share and run a new batch.
  The landing zone is append-only and duplicates are detected by sha256.
- A quarantined file is a data-quality outcome, not a pipeline bug: report the failing checks
  from `abc.v_failed_checks` to the SAP extract owner.
- Do not print connection strings, DSNs or Key Vault values; refer to secrets by name.
- Exit code `1` with "already running" means another batch holds the advisory lock: wait, do not retry in a loop.

## Diagnostics

```sql
SELECT * FROM abc.v_batch_summary ORDER BY batch_id DESC LIMIT 5;
SELECT * FROM abc.v_failed_checks WHERE batch_id = :batch_id;
```
