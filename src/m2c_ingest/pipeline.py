"""Orchestrates discovery -> profile -> control checks -> land -> reconcile for one batch."""
from __future__ import annotations

import fnmatch
import getpass
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg

from .abc_repo import AbcRepository, Check
from .blob_sink import BlobSink
from .config import EntityConfig, PipelineConfig
from .profiler import Profile, profile_file

logger = logging.getLogger(__name__)

# Checks where a bounded number of offending rows can be tolerated (entity.max_bad_rows).
ROW_LEVEL_CHECKS = ("column_count", "amount_parseable", "key_not_null", "key_unique")


@dataclass
class Candidate:
    entity: EntityConfig
    path: Path


def discover(cfg: PipelineConfig, now: float | None = None) -> tuple[list[Candidate], list[Path]]:
    """Returns (ready files, deferred files). A file is ready once it has been quiet for
    stability_seconds and, when trigger_suffix is set, its trigger file exists."""
    now = now or time.time()
    ready, deferred = [], []
    if not cfg.source_root.is_dir():
        raise FileNotFoundError(f"Source root not reachable: {cfg.source_root}")
    names = sorted(p for p in cfg.source_root.iterdir() if p.is_file())
    for ent in cfg.entities.values():
        for path in names:
            if not fnmatch.fnmatch(path.name, ent.file_pattern):
                continue
            quiet = now - path.stat().st_mtime >= cfg.stability_seconds
            triggered = cfg.trigger_suffix is None or path.with_name(path.name + cfg.trigger_suffix).exists()
            if quiet and triggered:
                ready.append(Candidate(ent, path))
            else:
                deferred.append(path)
    return ready, deferred


def build_checks(prof: Profile, ent: EntityConfig) -> list[Check]:
    checks: list[Check] = []
    file_level = {
        "CONTROL": ["header_matches_contract", "encoding_valid", "trailer_present", "trailer_parseable",
                    "single_trailer", "trailer_is_last", "non_empty"],
    }
    for name in file_level["CONTROL"]:
        if name.startswith("header") and not ent.has_header:
            continue
        if name.startswith(("trailer", "single_trailer")) and not ent.trailer:
            continue
        issues = [i for i in prof.issues if i.check == name]
        checks.append(Check("CONTROL", name, "FAIL" if issues else "PASS",
                            detail="; ".join(i.message for i in issues) or None))

    row_issues = [i for i in prof.issues if i.check in ROW_LEVEL_CHECKS]
    checks.append(Check(
        "CONTROL", "bad_row_threshold",
        "FAIL" if prof.bad_rows > ent.max_bad_rows else ("WARN" if prof.bad_rows else "PASS"),
        expected=f"<= {ent.max_bad_rows}", actual=prof.bad_rows,
        detail="; ".join(f"line {i.line_no}: {i.check} {i.message}" for i in row_issues) or None,
    ))

    if ent.trailer and prof.trailer_row_count is not None:
        diff = prof.row_count - prof.trailer_row_count
        checks.append(Check("BALANCE", "row_count_vs_trailer", "PASS" if diff == 0 else "FAIL",
                            expected=prof.trailer_row_count, actual=prof.row_count,
                            variance=Decimal(diff), tolerance=Decimal(0)))
    for ct in ent.control_totals:
        if ct.name not in prof.trailer_totals:
            continue
        expected, actual = prof.trailer_totals[ct.name], prof.totals[ct.name]
        variance = actual - expected
        checks.append(Check("BALANCE", f"control_total:{ct.name}",
                            "PASS" if abs(variance) <= ct.tolerance else "FAIL",
                            expected=expected, actual=actual, variance=variance, tolerance=ct.tolerance))
    return checks


def verify_target(sink: BlobSink, container: str, blob_path: str, prof: Profile,
                  full: bool) -> list[Check]:
    state = sink.inspect(container, blob_path, full=full)
    if not state.exists:
        return [Check("BALANCE", "target_exists", "FAIL", detail=f"{container}/{blob_path} missing")]
    checks = [
        Check("BALANCE", "target_exists", "PASS"),
        Check("BALANCE", "target_size_bytes", "PASS" if state.size == prof.size_bytes else "FAIL",
              expected=prof.size_bytes, actual=state.size, variance=Decimal((state.size or 0) - prof.size_bytes)),
        Check("BALANCE", "target_content_md5", "PASS" if state.md5_b64 == prof.md5_b64 else "FAIL",
              expected=prof.md5_b64, actual=state.md5_b64),
        Check("AUDIT", "target_lineage_metadata",
              "PASS" if (state.metadata or {}).get("sha256") == prof.sha256 else "FAIL",
              expected=prof.sha256, actual=(state.metadata or {}).get("sha256")),
    ]
    if full:
        checks += [
            Check("BALANCE", "target_sha256", "PASS" if state.sha256 == prof.sha256 else "FAIL",
                  expected=prof.sha256, actual=state.sha256),
            Check("BALANCE", "target_line_count", "PASS" if state.line_count == prof.newline_count else "FAIL",
                  expected=prof.newline_count, actual=state.line_count,
                  variance=Decimal((state.line_count or 0) - prof.newline_count)),
        ]
    return checks


class IngestionPipeline:
    def __init__(self, cfg: PipelineConfig, repo: AbcRepository, sink: BlobSink):
        self.cfg, self.repo, self.sink = cfg, repo, sink

    def blob_path(self, ent: EntityConfig, path: Path, batch_id: int, load_date: str) -> str:
        return self.cfg.storage.path_template.format(
            entity=ent.name, sap_table=ent.sap_table, source_system=self.cfg.source_system,
            load_date=load_date, batch_id=batch_id, file_name=path.name)

    def run(self, triggered_by: str | None = None) -> dict:
        with self.repo.pipeline_lock(self.cfg.name):
            batch_id = self.repo.open_batch(self.cfg.name, self.cfg.source_system, self.cfg.version,
                                            self.cfg.snapshot, triggered_by or getpass.getuser())
            logger.info("Opened batch %s for pipeline %s", batch_id, self.cfg.name)
            error = None
            try:
                self.sink.ensure_containers()
                ready, deferred = discover(self.cfg)
                for p in deferred:
                    logger.info("Deferred (not stable or no trigger file yet): %s", p)
                load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                with ThreadPoolExecutor(max_workers=max(1, self.cfg.workers)) as pool:
                    list(pool.map(lambda c: self._process(batch_id, c, load_date), ready))
            except Exception as e:  # batch-level failure (share unreachable, storage down, ...)
                logger.exception("Batch %s aborted", batch_id)
                error = f"{type(e).__name__}: {e}"
            status = self.repo.close_batch(batch_id, error)
            logger.info("Closed batch %s with status %s", batch_id, status)
            return {"batch_id": batch_id, "status": status, "deferred": [str(p) for p in deferred]
                    if error is None else []}

    def _process(self, batch_id: int, cand: Candidate, load_date: str) -> str:
        ent, path = cand.entity, cand.path
        st = path.stat()
        fid = self.repo.start_file(batch_id, ent.name, ent.sap_table, path, st.st_size, st.st_mtime)
        try:
            prof = profile_file(path, ent)
            self.repo.update_file(fid, status="PROFILED", sha256=prof.sha256, md5_b64=prof.md5_b64,
                                  file_size_bytes=prof.size_bytes, source_row_count=prof.row_count,
                                  trailer_row_count=prof.trailer_row_count)

            prior = self.repo.already_reconciled(ent.name, prof.sha256)
            if prior:
                self.repo.record_checks(batch_id, fid, [Check(
                    "AUDIT", "not_previously_ingested", "WARN", expected="new content",
                    actual=f"identical to file_audit_id {prior}")])
                self.repo.update_file(fid, status="SKIPPED_DUPLICATE")
                return "SKIPPED_DUPLICATE"

            checks = [Check("AUDIT", "not_previously_ingested", "PASS"),
                      Check("AUDIT", "file_stable", "PASS",
                            detail=f"quiet >= {self.cfg.stability_seconds}s")] + build_checks(prof, ent)
            failed = any(c.status == "FAIL" for c in checks)
            container = self.cfg.storage.quarantine_container if failed else self.cfg.storage.container
            blob_path = self.blob_path(ent, path, batch_id, load_date)
            meta = {"batch_id": str(batch_id), "file_audit_id": str(fid), "entity": ent.name,
                    "sap_table": ent.sap_table, "source_system": self.cfg.source_system,
                    "sha256": prof.sha256, "row_count": str(prof.row_count),
                    "source_file": path.name.encode("ascii", "replace").decode()}
            landed = self.sink.upload(path, container, blob_path, prof.md5_b64, meta)
            self.repo.update_file(fid, target_container=container, target_blob_path=blob_path,
                                  target_etag=landed.etag, target_size_bytes=landed.size,
                                  status="LANDED" if not failed else "PROFILED")
            if failed:
                self.repo.record_checks(batch_id, fid, checks)
                self.repo.update_file(fid, status="QUARANTINED",
                                      error_message="; ".join(f"{c.layer}.{c.name}" for c in checks
                                                              if c.status == "FAIL"))
                logger.warning("Quarantined %s", path.name)
                return "QUARANTINED"

            checks += verify_target(self.sink, container, blob_path, prof,
                                    full=self.cfg.storage.verify_mode == "full")
            self.repo.record_checks(batch_id, fid, checks)
            bad = [f"{c.layer}.{c.name}" for c in checks if c.status == "FAIL"]
            if bad:
                self.repo.update_file(fid, status="FAILED", error_message="; ".join(bad))
                return "FAILED"
            self.repo.update_file(fid, status="RECONCILED")
            self._post_success(path)
            logger.info("Reconciled %s -> %s/%s (%s rows)", path.name, container, blob_path, prof.row_count)
            return "RECONCILED"
        except psycopg.errors.UniqueViolation:
            # Same content reconciled concurrently by another worker in this batch.
            self.repo.update_file(fid, status="FAILED", error_message="concurrent duplicate content")
            return "FAILED"
        except Exception as e:
            logger.exception("File %s failed", path)
            self.repo.update_file(fid, status="FAILED", error_message=f"{type(e).__name__}: {e}"[:2000])
            return "FAILED"

    def _post_success(self, path: Path) -> None:
        if self.cfg.post_success_action != "archive":
            return
        dest = path.parent / self.cfg.archive_dir
        dest.mkdir(exist_ok=True)
        shutil.move(str(path), dest / path.name)
        if self.cfg.trigger_suffix:
            trig = path.with_name(path.name + self.cfg.trigger_suffix)
            if trig.exists():
                shutil.move(str(trig), dest / trig.name)
