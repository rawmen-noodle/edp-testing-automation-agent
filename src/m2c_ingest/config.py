"""Load the pipeline + entity catalog YAML and resolve ${ENV} references."""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

_ENV_REF = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


class ConfigError(ValueError):
    pass


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            resolved = os.getenv(name, default)
            if resolved is None:
                raise ConfigError(f"Environment variable '{name}' is referenced in config but not set")
            return resolved
        return _ENV_REF.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


@dataclass(frozen=True)
class ControlTotal:
    name: str
    column: str
    tolerance: Decimal = Decimal("0")
    trailer_field: int | None = None  # index in the trailer record holding the SAP-side total


@dataclass(frozen=True)
class Trailer:
    prefix: str
    row_count_field: int


@dataclass(frozen=True)
class EntityConfig:
    name: str
    sap_table: str
    file_pattern: str
    expected_columns: tuple[str, ...]
    key_columns: tuple[str, ...] = ()
    delimiter: str = "|"
    encoding: str = "utf-8"
    has_header: bool = True
    trailer: Trailer | None = None
    control_totals: tuple[ControlTotal, ...] = ()
    allow_empty: bool = False
    check_duplicate_keys: bool = False
    max_bad_rows: int = 0


@dataclass(frozen=True)
class StorageConfig:
    container: str
    quarantine_container: str
    path_template: str
    account_url: str | None = None
    connection_string_secret: str | None = None
    max_concurrency: int = 4
    verify_mode: str = "metadata"  # metadata | full


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    source_system: str
    source_root: Path
    storage: StorageConfig
    abc_dsn_secret: str
    secrets_provider: str
    stability_seconds: int
    trigger_suffix: str | None
    post_success_action: str  # none | archive
    archive_dir: str
    workers: int
    entities: dict[str, EntityConfig] = field(default_factory=dict)
    version: str = ""
    snapshot: dict = field(default_factory=dict)


def _entity(name: str, raw: dict) -> EntityConfig:
    try:
        trailer = raw.get("trailer")
        return EntityConfig(
            name=name,
            sap_table=raw["sap_table"],
            file_pattern=raw["file_pattern"],
            expected_columns=tuple(raw["expected_columns"]),
            key_columns=tuple(raw.get("key_columns", ())),
            delimiter=raw.get("delimiter", "|"),
            encoding=raw.get("encoding", "utf-8"),
            has_header=raw.get("has_header", True),
            trailer=Trailer(trailer["prefix"], int(trailer["row_count_field"])) if trailer else None,
            control_totals=tuple(
                ControlTotal(
                    name=ct["name"],
                    column=ct["column"],
                    tolerance=Decimal(str(ct.get("tolerance", 0))),
                    trailer_field=ct.get("trailer_field"),
                )
                for ct in raw.get("control_totals", ())
            ),
            allow_empty=raw.get("allow_empty", False),
            check_duplicate_keys=raw.get("check_duplicate_keys", False),
            max_bad_rows=int(raw.get("max_bad_rows", 0)),
        )
    except KeyError as e:
        raise ConfigError(f"Entity '{name}' is missing required key {e}") from None


def load_config(path: str | Path) -> PipelineConfig:
    text = Path(path).read_text(encoding="utf-8")
    raw = _expand(yaml.safe_load(text))
    p = raw["pipeline"]
    s = p["storage"]
    if not s.get("account_url") and not s.get("connection_string_secret"):
        raise ConfigError("storage needs either account_url (managed identity) or connection_string_secret")
    if s.get("verify_mode", "metadata") not in ("metadata", "full"):
        raise ConfigError("storage.verify_mode must be 'metadata' or 'full'")

    entities = {name: _entity(name, e) for name, e in raw["entities"].items()}
    for ent in entities.values():
        for col in ent.key_columns + tuple(ct.column for ct in ent.control_totals):
            if col not in ent.expected_columns:
                raise ConfigError(f"Entity '{ent.name}': column '{col}' not in expected_columns")

    return PipelineConfig(
        name=p["name"],
        source_system=p.get("source_system", "SAP"),
        source_root=Path(p["source_root"]),
        storage=StorageConfig(
            container=s["container"],
            quarantine_container=s["quarantine_container"],
            path_template=s["path_template"],
            account_url=s.get("account_url") or None,
            connection_string_secret=s.get("connection_string_secret") or None,
            max_concurrency=int(s.get("max_concurrency", 4)),
            verify_mode=s.get("verify_mode", "metadata"),
        ),
        abc_dsn_secret=p["abc"]["dsn_secret"],
        secrets_provider=p.get("secrets_provider", "keyvault"),
        stability_seconds=int(p.get("stability_seconds", 30)),
        trigger_suffix=p.get("trigger_suffix") or None,
        post_success_action=p.get("post_success_action", "none"),
        archive_dir=p.get("archive_dir", "processed"),
        workers=int(p.get("workers", 1)),
        entities=entities,
        version=hashlib.sha256(text.encode()).hexdigest()[:12],
        snapshot=json.loads(json.dumps(raw, default=str)),
    )
