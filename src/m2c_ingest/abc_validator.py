"""Independent validation of the ABC process for one batch.

The pipeline writes the ABC records; this module re-derives every claim those records
make, from PostgreSQL and from Azure Storage directly, and fails if any disagree."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .abc_repo import TERMINAL_FILE_STATUSES, AbcRepository
from .blob_sink import BlobSink


@dataclass(frozen=True)
class SqlRule:
    rule_id: str
    description: str
    sql: str  # returns offending rows; parameter %(b)s = batch_id


SQL_RULES: tuple[SqlRule, ...] = (
    SqlRule("A01", "Batch is closed with a terminal status and end time",
            "SELECT batch_id, status, ended_at FROM abc.batch_run WHERE batch_id=%(b)s "
            "AND (status='RUNNING' OR ended_at IS NULL)"),
    SqlRule("A02", "Every file in the batch reached a terminal status",
            "SELECT file_audit_id, file_name, status FROM abc.file_audit WHERE batch_id=%(b)s "
            f"AND status NOT IN {TERMINAL_FILE_STATUSES}"),
    SqlRule("A03", "Every processed file carries a sha256 fingerprint and size",
            "SELECT file_audit_id, file_name FROM abc.file_audit WHERE batch_id=%(b)s "
            "AND status <> 'FAILED' AND (sha256 IS NULL OR file_size_bytes IS NULL)"),
    SqlRule("A04", "Batch counters equal the sum of its file audit rows",
            """SELECT b.batch_id, b.files_discovered, f.n, b.files_succeeded, f.ok, b.rows_landed, f.rows
               FROM abc.batch_run b,
                    LATERAL (SELECT count(*) n, count(*) FILTER (WHERE status='RECONCILED') ok,
                                    coalesce(sum(source_row_count) FILTER (WHERE status='RECONCILED'),0) rows
                             FROM abc.file_audit WHERE batch_id=b.batch_id) f
               WHERE b.batch_id=%(b)s AND (b.files_discovered<>f.n OR b.files_succeeded<>f.ok
                                          OR b.rows_landed<>f.rows)"""),
    SqlRule("B01", "Reconciled files: source row count equals SAP trailer row count",
            "SELECT file_audit_id, file_name, source_row_count, trailer_row_count FROM abc.file_audit "
            "WHERE batch_id=%(b)s AND status='RECONCILED' AND trailer_row_count IS NOT NULL "
            "AND source_row_count <> trailer_row_count"),
    SqlRule("B02", "Reconciled files: landed blob size equals source size",
            "SELECT file_audit_id, file_name, file_size_bytes, target_size_bytes FROM abc.file_audit "
            "WHERE batch_id=%(b)s AND status='RECONCILED' "
            "AND target_size_bytes IS DISTINCT FROM file_size_bytes"),
    SqlRule("B03", "Reconciled files have zero failed checks",
            "SELECT f.file_audit_id, f.file_name, c.check_name FROM abc.file_audit f "
            "JOIN abc.control_check c USING (file_audit_id) "
            "WHERE f.batch_id=%(b)s AND f.status='RECONCILED' AND c.status='FAIL'"),
    SqlRule("B04", "Reconciled files passed every mandatory balance check",
            """SELECT f.file_audit_id, f.file_name, m.check_name AS missing_check
               FROM abc.file_audit f
               CROSS JOIN (VALUES ('target_exists'),('target_size_bytes'),('target_content_md5')) m(check_name)
               WHERE f.batch_id=%(b)s AND f.status='RECONCILED' AND NOT EXISTS (
                 SELECT 1 FROM abc.control_check c WHERE c.file_audit_id=f.file_audit_id
                   AND c.check_name=m.check_name AND c.status='PASS')"""),
    SqlRule("B05", "Control-total variances are within their tolerance",
            "SELECT c.file_audit_id, c.check_name, c.variance, c.tolerance FROM abc.control_check c "
            "WHERE c.batch_id=%(b)s AND c.check_name LIKE 'control_total:%%' "
            "AND (c.status='PASS') <> (abs(c.variance) <= c.tolerance)"),
    SqlRule("C01", "Every quarantined or failed file has at least one failed check or error",
            "SELECT f.file_audit_id, f.file_name, f.status FROM abc.file_audit f "
            "WHERE f.batch_id=%(b)s AND f.status IN ('QUARANTINED','FAILED') AND f.error_message IS NULL "
            "AND NOT EXISTS (SELECT 1 FROM abc.control_check c WHERE c.file_audit_id=f.file_audit_id "
            "AND c.status='FAIL')"),
    SqlRule("C02", "Quarantined data never lands in the clean landing container",
            "SELECT f.file_audit_id, f.file_name, f.target_container FROM abc.file_audit f "
            "JOIN abc.batch_run b USING (batch_id) WHERE f.batch_id=%(b)s AND f.status='QUARANTINED' "
            "AND f.target_container = b.config_snapshot->'pipeline'->'storage'->>'container'"),
    SqlRule("C03", "No content (entity + sha256) is reconciled more than once across all batches",
            "SELECT entity, sha256, count(*) n FROM abc.file_audit WHERE status='RECONCILED' "
            "AND (entity, sha256) IN (SELECT entity, sha256 FROM abc.file_audit WHERE batch_id=%(b)s) "
            "GROUP BY entity, sha256 HAVING count(*) > 1"),
    SqlRule("C04", "Skipped duplicates point at an earlier reconciled file",
            "SELECT f.file_audit_id, f.file_name FROM abc.file_audit f WHERE f.batch_id=%(b)s "
            "AND f.status='SKIPPED_DUPLICATE' AND NOT EXISTS (SELECT 1 FROM abc.file_audit p "
            "WHERE p.entity=f.entity AND p.sha256=f.sha256 AND p.status='RECONCILED' "
            "AND p.file_audit_id < f.file_audit_id)"),
    SqlRule("C05", "Batch status is consistent with its file outcomes",
            """SELECT b.batch_id, b.status, b.files_succeeded, b.files_discovered, b.files_skipped
               FROM abc.batch_run b WHERE b.batch_id=%(b)s AND b.error_message IS NULL AND NOT (
                 (b.status='NO_DATA'   AND b.files_discovered=b.files_skipped) OR
                 (b.status='SUCCEEDED' AND b.files_discovered>b.files_skipped
                                       AND b.files_succeeded=b.files_discovered-b.files_skipped) OR
                 (b.status='PARTIAL'   AND b.files_succeeded>0
                                       AND b.files_succeeded<b.files_discovered-b.files_skipped) OR
                 (b.status='FAILED'    AND b.files_succeeded=0 AND b.files_discovered>b.files_skipped))"""),
)


def _result(rule_id: str, description: str, offending: list) -> dict:
    return {"rule_id": rule_id, "description": description,
            "status": "FAIL" if offending else "PASS",
            "offending_count": len(offending), "sample": offending[:5]}


def validate_batch(repo: AbcRepository, sink: BlobSink | None, batch_id: int,
                   verify_mode: str = "metadata", landing_container: str | None = None,
                   progress: Callable[[str], None] | None = None) -> dict:
    results = []
    for rule in SQL_RULES:
        rows = repo.query(rule.sql, {"b": batch_id})
        results.append(_result(rule.rule_id, rule.description, rows))
        if progress:
            progress(rule.rule_id)

    if sink is not None:
        files = repo.query(
            "SELECT file_audit_id, file_name, sha256, md5_b64, file_size_bytes, target_container, "
            "target_blob_path FROM abc.file_audit WHERE batch_id=%(b)s AND status='RECONCILED'",
            {"b": batch_id})
        missing, mismatched = [], []
        for f in files:
            state = sink.inspect(f["target_container"], f["target_blob_path"], full=verify_mode == "full")
            if not state.exists:
                missing.append({"file_audit_id": f["file_audit_id"], "blob": f["target_blob_path"]})
                continue
            problems = []
            if state.size != f["file_size_bytes"]:
                problems.append(f"size {state.size} != {f['file_size_bytes']}")
            if state.md5_b64 != f["md5_b64"]:
                problems.append("content_md5 differs")
            if (state.metadata or {}).get("sha256") != f["sha256"]:
                problems.append("lineage sha256 metadata differs")
            if verify_mode == "full" and state.sha256 != f["sha256"]:
                problems.append("downloaded content sha256 differs")
            if problems:
                mismatched.append({"file_audit_id": f["file_audit_id"], "file": f["file_name"],
                                   "problems": problems})
        results.append(_result("S01", "Every reconciled file still exists in Azure Storage", missing))
        results.append(_result("S02", f"Stored blobs match the audited fingerprint ({verify_mode} mode)",
                               mismatched))

        if landing_container:
            recorded = {f["target_blob_path"] for f in files}
            prefixes = {p.rsplit("/", 1)[0] + "/" for p in recorded}
            orphans = [p for pre in prefixes for p in sink.list_paths(landing_container, pre)
                       if p not in recorded]
            results.append(_result("S03", "No unaudited (orphan) blobs under this batch's landing paths",
                                   orphans))

    validation_id = repo.save_validation(batch_id, verify_mode, results)
    failed = [r for r in results if r["status"] == "FAIL"]
    return {"validation_id": validation_id, "batch_id": batch_id,
            "overall_status": "FAIL" if failed else "PASS",
            "checks_total": len(results), "checks_failed": len(failed), "results": results}
