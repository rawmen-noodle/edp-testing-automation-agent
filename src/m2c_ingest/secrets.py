"""Secret resolution. Production uses Azure Key Vault via DefaultAzureCredential;
local/test runs can use environment variables. Secret values are never logged."""
from __future__ import annotations

import logging
import os
from functools import lru_cache

logger = logging.getLogger(__name__)


class SecretProvider:
    def get(self, name: str) -> str:
        raise NotImplementedError


class EnvSecretProvider(SecretProvider):
    """Maps secret 'abc-postgres-dsn' to env var ABC_POSTGRES_DSN."""

    def get(self, name: str) -> str:
        var = name.upper().replace("-", "_")
        value = os.getenv(var)
        if not value:
            raise KeyError(f"Secret '{name}' not found (expected env var {var})")
        return value


class KeyVaultSecretProvider(SecretProvider):
    def __init__(self, vault_url: str):
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        # Managed identity on Azure hosts; az CLI / env service principal elsewhere.
        self._client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
        self._vault_url = vault_url

    @lru_cache(maxsize=32)
    def get(self, name: str) -> str:
        logger.info("Resolving secret '%s' from %s", name, self._vault_url)
        return self._client.get_secret(name).value


def build_provider(kind: str) -> SecretProvider:
    if kind == "env":
        return EnvSecretProvider()
    if kind == "keyvault":
        vault_url = os.getenv("AZURE_KEYVAULT_URL")
        if not vault_url:
            raise RuntimeError("secrets_provider is 'keyvault' but AZURE_KEYVAULT_URL is not set")
        return KeyVaultSecretProvider(vault_url)
    raise ValueError(f"Unknown secrets provider '{kind}'")
