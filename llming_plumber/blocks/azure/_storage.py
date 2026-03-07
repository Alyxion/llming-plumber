"""Shared helpers for Azure Blob Storage blocks."""

from __future__ import annotations

import os

from azure.storage.blob.aio import BlobServiceClient


def get_blob_service(connection_string: str = "") -> BlobServiceClient:
    """Return an async BlobServiceClient.

    Falls back to AZURE_STORAGE_CONNECTION_STRING env var.
    """
    conn_str = connection_string or os.environ.get(
        "AZURE_STORAGE_CONNECTION_STRING", "",
    )
    if not conn_str:
        msg = (
            "No connection string provided and "
            "AZURE_STORAGE_CONNECTION_STRING is not set"
        )
        raise ValueError(msg)
    return BlobServiceClient.from_connection_string(conn_str)
