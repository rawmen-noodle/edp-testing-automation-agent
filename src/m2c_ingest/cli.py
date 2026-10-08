"""Command line entry point: init-db | ingest | validate-abc.

Exit codes: 0 success, 1 failure, 2 partial success / validation failed, 3 configuration error."""
from __future__ import annotations

import argparse
import json
import logging
import sys

from .abc_repo import AbcRepository
from .abc_validator import validate_batch
from .blob_sink import BlobSink, build_service_client
from .config import ConfigError, load_config
from .pipeline import IngestionPipeline
from .secrets import build_provider

EXIT = {"SUCCEEDED": 0, "NO_DATA": 0, "PARTIAL": 2, "FAILED": 1}


def _setup_logging(level: str) -> None:
    logging.basicConfig(level=level, stream=sys.stderr,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    # The Azure SDK logs full HTTP traffic at INFO; keep it out of pipeline logs.
    logging.getLogger("azure").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="m2c-ingest", description=__doc__)
    ap.add_argument("--config", required=True, help="Pipeline + entity catalog YAML")
    ap.add_argument("--log-level", default="INFO")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="Create or upgrade the ABC schema")
    ing = sub.add_parser("ingest", help="Run one ingestion batch")
    ing.add_argument("--triggered-by", default=None)
    val = sub.add_parser("validate-abc", help="Independently validate a batch's ABC records")
    val.add_argument("--batch-id", type=int, default=None, help="Defaults to the latest batch")
    val.add_argument("--verify-mode", choices=("metadata", "full"), default=None)
    val.add_argument("--skip-storage", action="store_true", help="Database rules only")
    args = ap.parse_args(argv)
    _setup_logging(args.log_level)
    log = logging.getLogger("m2c_ingest")

    try:
        cfg = load_config(args.config)
        secrets = build_provider(cfg.secrets_provider)
        repo = AbcRepository(secrets.get(cfg.abc_dsn_secret))
    except (ConfigError, KeyError, RuntimeError, ValueError) as e:
        log.error("Configuration error: %s", e)
        return 3

    if args.cmd == "init-db":
        repo.apply_schema()
        log.info("ABC schema applied")
        return 0

    sink = BlobSink(build_service_client(cfg.storage, secrets), cfg.storage)

    if args.cmd == "ingest":
        result = IngestionPipeline(cfg, repo, sink).run(args.triggered_by)
        print(json.dumps(result, default=str))
        return EXIT[result["status"]]

    batch_id = args.batch_id or repo.latest_batch_id(cfg.name)
    if batch_id is None:
        log.error("No batches recorded for pipeline %s", cfg.name)
        return 1
    report = validate_batch(repo, None if args.skip_storage else sink, batch_id,
                            verify_mode=args.verify_mode or cfg.storage.verify_mode,
                            landing_container=cfg.storage.container)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["overall_status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
