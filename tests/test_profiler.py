"""Infrastructure-free tests: config loading, discovery and the streaming profiler."""
from __future__ import annotations

import hashlib
import os
import time
from decimal import Decimal

import pytest
from generate_m2c_samples import generate

from m2c_ingest.config import ConfigError, load_config
from m2c_ingest.pipeline import build_checks, discover
from m2c_ingest.profiler import parse_sap_amount, profile_file

from conftest import CONFIG


@pytest.fixture
def cfg(monkeypatch, share):
    monkeypatch.setenv("M2C_SOURCE_ROOT", str(share))
    monkeypatch.setenv("M2C_STABILITY_SECONDS", "0")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_URL", "https://example.blob.core.windows.net")
    return load_config(CONFIG)


@pytest.mark.parametrize("raw,expected", [
    ("1234.50", "1234.50"), ("1234.50-", "-1234.50"), ("1,234.50-", "-1234.50"), ("", "0"), (" 7.00 ", "7.00"),
])
def test_parse_sap_amount(raw, expected):
    assert parse_sap_amount(raw) == Decimal(expected)


def test_config_requires_storage_auth(monkeypatch, share, tmp_path):
    monkeypatch.setenv("M2C_SOURCE_ROOT", str(share))
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.delenv("M2C_STORAGE_SECRET_NAME", raising=False)
    with pytest.raises(ConfigError, match="account_url"):
        load_config(CONFIG)


def test_config_rejects_unknown_control_column(cfg, tmp_path, monkeypatch):
    bad = tmp_path / "bad.yaml"
    bad.write_text(CONFIG.read_text().replace("column: DMBTR", "column: WRBTR"))
    with pytest.raises(ConfigError, match="WRBTR"):
        load_config(bad)


def test_missing_env_reference_is_an_error(monkeypatch, tmp_path):
    monkeypatch.delenv("M2C_SOURCE_ROOT", raising=False)
    with pytest.raises(ConfigError, match="M2C_SOURCE_ROOT"):
        load_config(CONFIG)


def test_clean_files_profile_without_issues(cfg, share):
    files = generate(share, "clean", rows=500)
    by_table = {e.sap_table: e for e in cfg.entities.values()}
    for f in files:
        ent = by_table[f.name.split("_")[0]]
        prof = profile_file(f, ent)
        assert prof.issues == [], (f.name, prof.issues)
        assert prof.sha256 == hashlib.sha256(f.read_bytes()).hexdigest()
        assert prof.size_bytes == f.stat().st_size
        assert prof.row_count == prof.trailer_row_count
        assert all(c.status == "PASS" for c in build_checks(prof, ent)), f.name


def test_faulty_files_raise_expected_checks(cfg, share):
    expected = {
        "VBAK_20261007_101.txt": "row_count_vs_trailer",
        "VBAP_20261007_102.txt": "control_total:sum_netwr",
        "KNA1_20261007_103.txt": "header_matches_contract",
        "VBAK_20261007_104.txt": "bad_row_threshold",
        "VBRK_20261007_105.txt": "encoding_valid",
        "BSID_20261007_106.txt": "trailer_present",
    }
    files = generate(share, "faulty", rows=200)
    by_table = {e.sap_table: e for e in cfg.entities.values()}
    for f in files:
        ent = by_table[f.name.split("_")[0]]
        failed = {c.name for c in build_checks(profile_file(f, ent), ent) if c.status == "FAIL"}
        assert expected[f.name] in failed, (f.name, failed)


def test_control_total_variance_is_exact(cfg, share):
    f = [p for p in generate(share, "faulty", rows=200) if p.name.endswith("_102.txt")][0]
    ent = cfg.entities["sales_order_item"]
    chk = {c.name: c for c in build_checks(profile_file(f, ent), ent)}
    assert chk["control_total:sum_netwr"].variance == Decimal("-10.00")
    assert chk["control_total:sum_kwmeng"].status == "PASS"


def test_discovery_waits_for_stability_and_trigger(cfg, share, monkeypatch, tmp_path):
    f = generate(share, "clean", rows=10)[0]
    os.utime(f, (time.time(), time.time()))
    cfg2 = cfg.__class__(**{**cfg.__dict__, "stability_seconds": 300, "trigger_suffix": ".done"})
    ready, deferred = discover(cfg2)
    assert f in deferred and not any(c.path == f for c in ready)
    (share / (f.name + ".done")).touch()
    ready, _ = discover(cfg2, now=time.time() + 301)
    assert any(c.path == f for c in ready)
