"""Integration fixtures run against a real PostgreSQL and a real Blob endpoint (Azurite locally,
or a sandbox storage account). Set:

    M2C_TEST_PG_DSN        e.g. postgresql://postgres@localhost:5432/edp_abc
    M2C_TEST_STORAGE_CONN  e.g. the Azurite development connection string

Tests that need them are skipped when they are not set."""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from m2c_ingest.abc_repo import AbcRepository  # noqa: E402
from m2c_ingest.blob_sink import BlobSink, build_service_client  # noqa: E402
from m2c_ingest.config import load_config  # noqa: E402
from m2c_ingest.secrets import EnvSecretProvider  # noqa: E402

CONFIG = ROOT / "tools-registry" / "config" / "sap_m2c_pipeline.yaml"


@pytest.fixture
def share(tmp_path) -> Path:
    d = tmp_path / "sap_outbound" / "m2c"
    d.mkdir(parents=True)
    return d


@pytest.fixture
def env(monkeypatch, share):
    pg = os.getenv("M2C_TEST_PG_DSN")
    st = os.getenv("M2C_TEST_STORAGE_CONN")
    if not pg or not st:
        pytest.skip("integration infrastructure not configured")
    monkeypatch.setenv("M2C_SOURCE_ROOT", str(share))
    monkeypatch.setenv("M2C_SECRETS_PROVIDER", "env")
    monkeypatch.setenv("M2C_STABILITY_SECONDS", "0")
    monkeypatch.setenv("M2C_STORAGE_SECRET_NAME", "storage-connection-string")
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", st)
    monkeypatch.setenv("ABC_POSTGRES_DSN", pg)
    return {"pg": pg, "storage": st}


@pytest.fixture
def stack(env, tmp_path):
    """Fresh ABC schema + uniquely named containers per test."""
    suffix = uuid.uuid4().hex[:8]
    cfg_text = CONFIG.read_text().replace("raw-landing-zone", f"landing-{suffix}") \
                                 .replace("raw-quarantine", f"quarantine-{suffix}")
    cfg_path = tmp_path / "pipeline.yaml"
    cfg_path.write_text(cfg_text)
    cfg = load_config(cfg_path)
    secrets = EnvSecretProvider()
    repo = AbcRepository(secrets.get(cfg.abc_dsn_secret))
    with repo.conn() as c:
        c.execute("DROP SCHEMA IF EXISTS abc CASCADE")
    repo.apply_schema()
    service = build_service_client(cfg.storage, secrets)
    sink = BlobSink(service, cfg.storage)
    yield {"cfg": cfg, "cfg_path": cfg_path, "repo": repo, "sink": sink, "service": service}
    for name in (cfg.storage.container, cfg.storage.quarantine_container):
        try:
            service.delete_container(name)
        except Exception:
            pass
