"""Azure Blob landing: streamed upload with content MD5 + lineage metadata, and
independent post-upload verification of what the storage service actually holds."""
from __future__ import annotations

import base64
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from .config import StorageConfig
from .secrets import SecretProvider

logger = logging.getLogger(__name__)


@dataclass
class LandedBlob:
    container: str
    path: str
    etag: str
    size: int
    url: str


@dataclass
class BlobState:
    exists: bool
    size: int | None = None
    md5_b64: str | None = None
    metadata: dict | None = None
    sha256: str | None = None      # only populated in full verification
    line_count: int | None = None  # only populated in full verification


def build_service_client(cfg: StorageConfig, secrets: SecretProvider) -> BlobServiceClient:
    if cfg.connection_string_secret:
        return BlobServiceClient.from_connection_string(secrets.get(cfg.connection_string_secret))
    from azure.identity import DefaultAzureCredential
    return BlobServiceClient(account_url=cfg.account_url, credential=DefaultAzureCredential())


class BlobSink:
    def __init__(self, service: BlobServiceClient, cfg: StorageConfig):
        self._svc = service
        self._cfg = cfg

    def ensure_containers(self) -> None:
        for name in (self._cfg.container, self._cfg.quarantine_container):
            try:
                self._svc.create_container(name)
            except ResourceExistsError:
                pass

    def upload(self, source: Path, container: str, blob_path: str, md5_b64: str,
               metadata: dict[str, str]) -> LandedBlob:
        blob = self._svc.get_blob_client(container=container, blob=blob_path)
        with open(source, "rb") as stream:
            # The SDK splits the stream into blocks; memory stays at block size x concurrency.
            result = blob.upload_blob(
                stream,
                overwrite=False,  # landing zone is append-only; re-runs get a new batch_id path
                max_concurrency=self._cfg.max_concurrency,
                content_settings=ContentSettings(
                    content_type="text/plain",
                    content_md5=bytearray(base64.b64decode(md5_b64)),
                ),
                metadata=metadata,
            )
        props = blob.get_blob_properties()
        return LandedBlob(container, blob_path, result["etag"], props.size, blob.url)

    def inspect(self, container: str, blob_path: str, full: bool = False) -> BlobState:
        blob = self._svc.get_blob_client(container=container, blob=blob_path)
        try:
            props = blob.get_blob_properties()
        except ResourceNotFoundError:
            return BlobState(exists=False)
        md5 = props.content_settings.content_md5
        state = BlobState(
            exists=True,
            size=props.size,
            md5_b64=base64.b64encode(bytes(md5)).decode() if md5 else None,
            metadata=dict(props.metadata or {}),
        )
        if full:
            sha, lines = hashlib.sha256(), 0
            for chunk in blob.download_blob(max_concurrency=self._cfg.max_concurrency).chunks():
                sha.update(chunk)
                lines += chunk.count(b"\n")
            state.sha256, state.line_count = sha.hexdigest(), lines
        return state

    def list_paths(self, container: str, prefix: str) -> list[str]:
        cc = self._svc.get_container_client(container)
        return [b.name for b in cc.list_blobs(name_starts_with=prefix)]
