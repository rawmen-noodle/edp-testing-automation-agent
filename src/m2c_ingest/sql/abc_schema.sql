-- Audit / Balance / Control (ABC) framework schema for SAP M2C landing pipelines.
-- Idempotent: safe to re-apply on every deployment.

CREATE SCHEMA IF NOT EXISTS abc;

-- One row per pipeline execution.
CREATE TABLE IF NOT EXISTS abc.batch_run (
    batch_id          BIGSERIAL PRIMARY KEY,
    pipeline_name     TEXT        NOT NULL,
    source_system     TEXT        NOT NULL,
    config_version    TEXT        NOT NULL,
    config_snapshot   JSONB       NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'RUNNING'
                      CHECK (status IN ('RUNNING','SUCCEEDED','PARTIAL','FAILED','NO_DATA')),
    run_host          TEXT        NOT NULL,
    triggered_by      TEXT        NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    files_discovered  INTEGER     NOT NULL DEFAULT 0,
    files_succeeded   INTEGER     NOT NULL DEFAULT 0,
    files_quarantined INTEGER     NOT NULL DEFAULT 0,
    files_failed      INTEGER     NOT NULL DEFAULT 0,
    files_skipped     INTEGER     NOT NULL DEFAULT 0,
    rows_landed       BIGINT      NOT NULL DEFAULT 0,
    bytes_landed      BIGINT      NOT NULL DEFAULT 0,
    error_message     TEXT
);

-- One row per source file per batch (the audit trail).
CREATE TABLE IF NOT EXISTS abc.file_audit (
    file_audit_id     BIGSERIAL PRIMARY KEY,
    batch_id          BIGINT      NOT NULL REFERENCES abc.batch_run(batch_id),
    entity            TEXT        NOT NULL,
    sap_table         TEXT        NOT NULL,
    source_path       TEXT        NOT NULL,
    file_name         TEXT        NOT NULL,
    file_size_bytes   BIGINT,
    file_mtime        TIMESTAMPTZ,
    sha256            CHAR(64),
    md5_b64           TEXT,
    source_row_count  BIGINT,
    trailer_row_count BIGINT,
    target_container  TEXT,
    target_blob_path  TEXT,
    target_etag       TEXT,
    target_size_bytes BIGINT,
    status            TEXT        NOT NULL DEFAULT 'DISCOVERED'
                      CHECK (status IN ('DISCOVERED','PROFILED','LANDED','RECONCILED',
                                        'QUARANTINED','FAILED','SKIPPED_DUPLICATE')),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    error_message     TEXT
);
CREATE INDEX IF NOT EXISTS ix_file_audit_batch  ON abc.file_audit(batch_id);
CREATE INDEX IF NOT EXISTS ix_file_audit_sha    ON abc.file_audit(entity, sha256);
-- Hard guarantee: the same content can only be reconciled once per entity.
CREATE UNIQUE INDEX IF NOT EXISTS ux_file_audit_reconciled_sha
    ON abc.file_audit(entity, sha256) WHERE status = 'RECONCILED';

-- Every audit, balance and control check evaluated for a file.
CREATE TABLE IF NOT EXISTS abc.control_check (
    check_id          BIGSERIAL PRIMARY KEY,
    batch_id          BIGINT      NOT NULL REFERENCES abc.batch_run(batch_id),
    file_audit_id     BIGINT      REFERENCES abc.file_audit(file_audit_id),
    check_layer       TEXT        NOT NULL CHECK (check_layer IN ('AUDIT','BALANCE','CONTROL')),
    check_name        TEXT        NOT NULL,
    expected_value    TEXT,
    actual_value      TEXT,
    variance          NUMERIC,
    tolerance         NUMERIC,
    status            TEXT        NOT NULL CHECK (status IN ('PASS','FAIL','WARN','SKIP')),
    detail            TEXT,
    checked_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_control_check_file ON abc.control_check(file_audit_id);
CREATE INDEX IF NOT EXISTS ix_control_check_batch ON abc.control_check(batch_id);

-- Independent post-run validation of the ABC process itself.
CREATE TABLE IF NOT EXISTS abc.validation_run (
    validation_id     BIGSERIAL PRIMARY KEY,
    batch_id          BIGINT      NOT NULL REFERENCES abc.batch_run(batch_id),
    verify_mode       TEXT        NOT NULL,
    overall_status    TEXT        NOT NULL CHECK (overall_status IN ('PASS','FAIL')),
    checks_total      INTEGER     NOT NULL,
    checks_failed     INTEGER     NOT NULL,
    validated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS abc.validation_result (
    validation_id     BIGINT      NOT NULL REFERENCES abc.validation_run(validation_id),
    rule_id           TEXT        NOT NULL,
    description       TEXT        NOT NULL,
    status            TEXT        NOT NULL CHECK (status IN ('PASS','FAIL')),
    offending_count   INTEGER     NOT NULL,
    sample            JSONB,
    PRIMARY KEY (validation_id, rule_id)
);

-- Operational views.
CREATE OR REPLACE VIEW abc.v_batch_summary AS
SELECT b.batch_id, b.pipeline_name, b.status, b.started_at, b.ended_at,
       b.files_discovered, b.files_succeeded, b.files_quarantined, b.files_failed,
       b.files_skipped, b.rows_landed, b.bytes_landed,
       count(c.*) FILTER (WHERE c.status = 'FAIL') AS failed_checks
FROM abc.batch_run b
LEFT JOIN abc.control_check c ON c.batch_id = b.batch_id
GROUP BY b.batch_id;

CREATE OR REPLACE VIEW abc.v_failed_checks AS
SELECT c.batch_id, f.entity, f.file_name, f.status AS file_status,
       c.check_layer, c.check_name, c.expected_value, c.actual_value,
       c.variance, c.detail, c.checked_at
FROM abc.control_check c
JOIN abc.file_audit f USING (file_audit_id)
WHERE c.status = 'FAIL';
