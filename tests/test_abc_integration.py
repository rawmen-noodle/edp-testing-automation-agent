"""End-to-end: network share -> pipeline -> Azure Blob (Azurite) -> ABC records in PostgreSQL,
then the independent ABC validator, including negative tests that tamper with storage and
with the ABC records to prove the validator catches it."""
from __future__ import annotations

import json
import shutil

import pytest
from generate_m2c_samples import generate

from m2c_ingest.abc_validator import validate_batch
from m2c_ingest.cli import main as cli_main
from m2c_ingest.pipeline import IngestionPipeline


def run(stack):
    return IngestionPipeline(stack["cfg"], stack["repo"], stack["sink"]).run("pytest")


def validate(stack, batch_id, mode="metadata"):
    return validate_batch(stack["repo"], stack["sink"], batch_id, verify_mode=mode,
                          landing_container=stack["cfg"].storage.container)


def failed_rules(report):
    return {r["rule_id"] for r in report["results"] if r["status"] == "FAIL"}


def file_statuses(stack, batch_id):
    rows = stack["repo"].query("SELECT file_name, status FROM abc.file_audit WHERE batch_id=%s "
                               "ORDER BY file_name", (batch_id,))
    return {r["file_name"]: r["status"] for r in rows}


@pytest.mark.parametrize("mode", ["metadata", "full"])
def test_clean_batch_reconciles_and_validates(stack, share, mode):
    generate(share, "clean", rows=2000)
    result = run(stack)
    assert result["status"] == "SUCCEEDED"
    assert set(file_statuses(stack, result["batch_id"]).values()) == {"RECONCILED"}

    batch = stack["repo"].query("SELECT * FROM abc.batch_run WHERE batch_id=%s", (result["batch_id"],))[0]
    assert batch["files_succeeded"] == 6
    assert batch["rows_landed"] == 2000 + 2000 + 6000 + 2000 + 2000 + 0

    report = validate(stack, result["batch_id"], mode)
    assert report["overall_status"] == "PASS", failed_rules(report)
    assert report["checks_total"] == 17


def test_blob_lands_with_lineage_metadata(stack, share):
    generate(share, "clean", rows=50)
    bid = run(stack)["batch_id"]
    row = stack["repo"].query("SELECT * FROM abc.file_audit WHERE batch_id=%s AND sap_table='VBAK'", (bid,))[0]
    assert row["target_blob_path"].startswith("sap/m2c/sales_order_header/load_date=")
    assert f"/batch_id={bid}/VBAK_20261007_001.txt" in row["target_blob_path"]
    state = stack["sink"].inspect(row["target_container"], row["target_blob_path"], full=True)
    assert state.metadata["sha256"] == row["sha256"] == state.sha256
    assert state.metadata["row_count"] == "50"


def test_redelivery_is_skipped_as_duplicate(stack, share):
    generate(share, "clean", rows=100)
    first = run(stack)
    second = run(stack)
    assert second["status"] == "NO_DATA"
    assert set(file_statuses(stack, second["batch_id"]).values()) == {"SKIPPED_DUPLICATE"}
    assert validate(stack, second["batch_id"])["overall_status"] == "PASS"
    assert validate(stack, first["batch_id"])["overall_status"] == "PASS"


def test_faulty_files_are_quarantined_and_abc_is_consistent(stack, share):
    generate(share, "faulty", rows=300)
    result = run(stack)
    assert result["status"] == "FAILED"
    assert set(file_statuses(stack, result["batch_id"]).values()) == {"QUARANTINED"}
    q = stack["cfg"].storage.quarantine_container
    assert len(stack["sink"].list_paths(q, "sap/m2c/")) == 6
    assert stack["sink"].list_paths(stack["cfg"].storage.container, "sap/m2c/") == []
    report = validate(stack, result["batch_id"])
    assert report["overall_status"] == "PASS", failed_rules(report)


def test_mixed_delivery_is_partial(stack, share):
    generate(share, "clean", rows=100)
    generate(share, "faulty", rows=100)
    result = run(stack)
    assert result["status"] == "PARTIAL"
    statuses = list(file_statuses(stack, result["batch_id"]).values())
    assert statuses.count("RECONCILED") == 6 and statuses.count("QUARANTINED") == 6
    assert validate(stack, result["batch_id"])["overall_status"] == "PASS"


def test_validator_detects_tampered_blob(stack, share):
    generate(share, "clean", rows=100)
    bid = run(stack)["batch_id"]
    row = stack["repo"].query("SELECT * FROM abc.file_audit WHERE batch_id=%s AND sap_table='BSID' "
                              "AND source_row_count > 0", (bid,))[0]
    blob = stack["service"].get_blob_client(row["target_container"], row["target_blob_path"])
    blob.upload_blob(b"tampered", overwrite=True)
    assert {"S02"} == failed_rules(validate(stack, bid))


def test_validator_detects_deleted_and_orphan_blobs(stack, share):
    generate(share, "clean", rows=100)
    bid = run(stack)["batch_id"]
    row = stack["repo"].query("SELECT * FROM abc.file_audit WHERE batch_id=%s AND sap_table='KNA1'", (bid,))[0]
    stack["service"].get_blob_client(row["target_container"], row["target_blob_path"]).delete_blob()
    prefix = row["target_blob_path"].rsplit("/", 1)[0]
    vbak = stack["repo"].query("SELECT target_blob_path FROM abc.file_audit WHERE batch_id=%s "
                               "AND sap_table='VBAK'", (bid,))[0]["target_blob_path"]
    stack["service"].get_blob_client(row["target_container"],
                                     vbak.rsplit("/", 1)[0] + "/UNAUDITED.txt").upload_blob(b"x")
    assert {"S01", "S03"} == failed_rules(validate(stack, bid)) - {"S02"}
    assert prefix  # deleted blob path recorded


def test_validator_detects_tampered_abc_records(stack, share):
    generate(share, "clean", rows=100)
    generate(share, "faulty", rows=100)
    bid = run(stack)["batch_id"]
    repo = stack["repo"]
    with repo.conn() as c:
        c.execute("UPDATE abc.batch_run SET rows_landed = rows_landed + 1, status='SUCCEEDED' "
                  "WHERE batch_id=%s", (bid,))
        c.execute("DELETE FROM abc.control_check WHERE file_audit_id IN (SELECT file_audit_id "
                  "FROM abc.file_audit WHERE batch_id=%s AND file_name='VBAK_20261007_101.txt')", (bid,))
        c.execute("UPDATE abc.file_audit SET error_message=NULL WHERE batch_id=%s "
                  "AND file_name='VBAK_20261007_101.txt'", (bid,))
        c.execute("UPDATE abc.file_audit SET status='LANDED' WHERE batch_id=%s "
                  "AND file_name='KNA1_20261007_001.txt'", (bid,))
    assert {"A02", "A04", "C01", "C05"} <= failed_rules(validate(stack, bid))


def test_concurrent_run_is_blocked(stack, share):
    with stack["repo"].pipeline_lock(stack["cfg"].name):
        with pytest.raises(RuntimeError, match="already running"):
            run(stack)


def test_archive_after_success(stack, share):
    generate(share, "clean", rows=10)
    cfg = stack["cfg"].__class__(**{**stack["cfg"].__dict__, "post_success_action": "archive"})
    IngestionPipeline(cfg, stack["repo"], stack["sink"]).run("pytest")
    assert sorted(p.name for p in share.glob("*.txt")) == []
    assert len(list((share / "processed").glob("*.txt"))) == 6


def test_cli_end_to_end(stack, share, capsys):
    generate(share, "clean", rows=100)
    cfg = str(stack["cfg_path"])
    assert cli_main(["--config", cfg, "init-db"]) == 0
    assert cli_main(["--config", cfg, "ingest", "--triggered-by", "ci"]) == 0
    ingest_out = json.loads(capsys.readouterr().out)
    assert cli_main(["--config", cfg, "validate-abc", "--verify-mode", "full"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["batch_id"] == ingest_out["batch_id"] and report["overall_status"] == "PASS"


def test_unreachable_share_fails_batch(stack, share):
    shutil.rmtree(share)
    result = run(stack)
    assert result["status"] == "FAILED"
    err = stack["repo"].query("SELECT error_message FROM abc.batch_run WHERE batch_id=%s",
                              (result["batch_id"],))[0]["error_message"]
    assert "Source root not reachable" in err
