"""PostgreSQL persistence for the Audit / Balance / Control framework.

Each write runs in its own short transaction so the audit trail survives a crash
mid-batch, and every worker thread opens its own connection."""
from __future__ import annotations

import json
import socket
import time
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

SCHEMA_SQL = Path(__file__).resolve().parent / "sql" / "abc_schema.sql"
TERMINAL_FILE_STATUSES = ("RECONCILED", "QUARANTINED", "FAILED", "SKIPPED_DUPLICATE")


@dataclass
class Check:
    layer: str
    name: str
    status: str
    expected: object = None
    actual: object = None
    variance: Decimal | None = None
    tolerance: Decimal | None = None
    detail: str | None = None


class AbcRepository:
    def __init__(self, dsn: str, connect_retries: int = 3):
        self._dsn = dsn
        self._retries = connect_retries

    @contextmanager
    def conn(self):
        delay = 1.0
        for attempt in range(1, self._retries + 1):
            try:
                c = psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=10)
                break
            except psycopg.OperationalError:
                if attempt == self._retries:
                    raise
                time.sleep(delay)
                delay *= 2
        try:
            with c.transaction():
                yield c
        finally:
            c.close()

    def apply_schema(self) -> None:
        with self.conn() as c:
            c.execute(SCHEMA_SQL.read_text())

    # ---- pipeline-level locking -------------------------------------------------
    @contextmanager
    def pipeline_lock(self, pipeline_name: str):
        """Session advisory lock: two runs of the same pipeline can never overlap."""
        key = zlib.crc32(pipeline_name.encode())
        c = psycopg.connect(self._dsn, autocommit=True)
        try:
            got = c.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
            if not got:
                raise RuntimeError(f"Pipeline '{pipeline_name}' is already running (advisory lock held)")
            yield
        finally:
            c.close()  # closing the session releases the lock

    # ---- batch ------------------------------------------------------------------
    def open_batch(self, pipeline_name: str, source_system: str, config_version: str,
                   snapshot: dict, triggered_by: str) -> int:
        with self.conn() as c:
            return c.execute(
                """INSERT INTO abc.batch_run (pipeline_name, source_system, config_version,
                       config_snapshot, run_host, triggered_by)
                   VALUES (%s,%s,%s,%s,%s,%s) RETURNING batch_id""",
                (pipeline_name, source_system, config_version, json.dumps(snapshot),
                 socket.gethostname(), triggered_by),
            ).fetchone()["batch_id"]

    def close_batch(self, batch_id: int, error: str | None = None) -> str:
        """Derives batch counters and status from file_audit so they cannot drift."""
        with self.conn() as c:
            agg = c.execute(
                """SELECT count(*) AS discovered,
                          count(*) FILTER (WHERE status='RECONCILED')        AS ok,
                          count(*) FILTER (WHERE status='QUARANTINED')       AS quarantined,
                          count(*) FILTER (WHERE status='FAILED')            AS failed,
                          count(*) FILTER (WHERE status='SKIPPED_DUPLICATE') AS skipped,
                          coalesce(sum(source_row_count) FILTER (WHERE status='RECONCILED'),0) AS rows,
                          coalesce(sum(target_size_bytes) FILTER (WHERE status='RECONCILED'),0) AS bytes
                   FROM abc.file_audit WHERE batch_id=%s""",
                (batch_id,),
            ).fetchone()
            if error:
                status = "FAILED"
            elif agg["discovered"] == agg["skipped"]:
                status = "NO_DATA"
            elif agg["ok"] == agg["discovered"] - agg["skipped"]:
                status = "SUCCEEDED"
            elif agg["ok"] == 0:
                status = "FAILED"
            else:
                status = "PARTIAL"
            c.execute(
                """UPDATE abc.batch_run SET status=%s, ended_at=now(), files_discovered=%s,
                       files_succeeded=%s, files_quarantined=%s, files_failed=%s, files_skipped=%s,
                       rows_landed=%s, bytes_landed=%s, error_message=%s
                   WHERE batch_id=%s""",
                (status, agg["discovered"], agg["ok"], agg["quarantined"], agg["failed"],
                 agg["skipped"], agg["rows"], agg["bytes"], error, batch_id),
            )
            return status

    # ---- files ------------------------------------------------------------------
    def start_file(self, batch_id: int, entity: str, sap_table: str, path: Path,
                   size: int, mtime_epoch: float) -> int:
        with self.conn() as c:
            return c.execute(
                """INSERT INTO abc.file_audit (batch_id, entity, sap_table, source_path, file_name,
                       file_size_bytes, file_mtime)
                   VALUES (%s,%s,%s,%s,%s,%s,to_timestamp(%s)) RETURNING file_audit_id""",
                (batch_id, entity, sap_table, str(path), path.name, size, mtime_epoch),
            ).fetchone()["file_audit_id"]

    def update_file(self, file_audit_id: int, **cols) -> None:
        if cols.get("status") in TERMINAL_FILE_STATUSES:
            cols.setdefault("ended_at", "now()")
        assignments, values = [], []
        for k, v in cols.items():
            if v == "now()":
                assignments.append(f"{k}=now()")
            else:
                assignments.append(f"{k}=%s")
                values.append(v)
        with self.conn() as c:
            c.execute(f"UPDATE abc.file_audit SET {', '.join(assignments)} WHERE file_audit_id=%s",
                      (*values, file_audit_id))

    def already_reconciled(self, entity: str, sha256: str) -> int | None:
        with self.conn() as c:
            row = c.execute(
                """SELECT file_audit_id FROM abc.file_audit
                   WHERE entity=%s AND sha256=%s AND status='RECONCILED' LIMIT 1""",
                (entity, sha256),
            ).fetchone()
            return row["file_audit_id"] if row else None

    def record_checks(self, batch_id: int, file_audit_id: int, checks: list[Check]) -> None:
        with self.conn() as c:
            with c.cursor() as cur:
                cur.executemany(
                    """INSERT INTO abc.control_check (batch_id, file_audit_id, check_layer, check_name,
                           expected_value, actual_value, variance, tolerance, status, detail)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [(batch_id, file_audit_id, k.layer, k.name,
                      None if k.expected is None else str(k.expected),
                      None if k.actual is None else str(k.actual),
                      k.variance, k.tolerance, k.status, k.detail) for k in checks],
                )

    # ---- reads used by the validator --------------------------------------------
    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self.conn() as c:
            return c.execute(sql, params).fetchall()

    def latest_batch_id(self, pipeline_name: str) -> int | None:
        rows = self.query("SELECT max(batch_id) AS b FROM abc.batch_run WHERE pipeline_name=%s",
                          (pipeline_name,))
        return rows[0]["b"]

    def save_validation(self, batch_id: int, verify_mode: str, results: list[dict]) -> int:
        failed = sum(1 for r in results if r["status"] == "FAIL")
        with self.conn() as c:
            vid = c.execute(
                """INSERT INTO abc.validation_run (batch_id, verify_mode, overall_status,
                       checks_total, checks_failed) VALUES (%s,%s,%s,%s,%s) RETURNING validation_id""",
                (batch_id, verify_mode, "FAIL" if failed else "PASS", len(results), failed),
            ).fetchone()["validation_id"]
            with c.cursor() as cur:
                cur.executemany(
                    """INSERT INTO abc.validation_result VALUES (%s,%s,%s,%s,%s,%s)""",
                    [(vid, r["rule_id"], r["description"], r["status"], r["offending_count"],
                      json.dumps(r["sample"], default=str)) for r in results],
                )
            return vid
