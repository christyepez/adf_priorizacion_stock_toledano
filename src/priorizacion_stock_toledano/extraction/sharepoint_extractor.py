from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import parse_qsl, quote, urljoin, urlparse


SHAREPOINT_METRICS_VIEW_NAME = "vw_metrics_ext_sharepoint_priorizacion_stock"
SHAREPOINT_SOURCE_SYSTEM = "sharepoint"
SENSITIVE_QUERY_KEYS = {"sig", "token", "access_token", "client_secret", "sharedaccesssignature"}


@dataclass(frozen=True)
class SharePointSecretNames:
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    tenant_id: str | None = None
    base_url: str | None = None


def _required(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Campo requerido ausente para SharePoint: {key}")
    return str(value).strip()


def reject_signed_or_secret_url(url: str) -> None:
    parsed = urlparse(url or "")
    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    if query_keys.intersection(SENSITIVE_QUERY_KEYS):
        raise ValueError("URL SharePoint no permitida: contiene firma, token o secreto embebido")


def build_bronze_path(
    *,
    storage_account: str,
    folder: str,
    filename: str,
    extension: str,
    container: str = "bronze",
) -> str:
    if not storage_account:
        raise ValueError("storage_account es obligatorio")
    clean_folder = (folder or "").strip().strip("/")
    clean_filename = _required({"filename": filename}, "filename")
    clean_extension = "" if extension is None else str(extension).strip()
    if clean_extension and not clean_extension.startswith("."):
        clean_extension = "." + clean_extension
    relative_path = f"{clean_folder}/{clean_filename}{clean_extension}" if clean_folder else f"{clean_filename}{clean_extension}"
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{relative_path}"


def build_source_file_name(record: Mapping[str, Any]) -> str:
    source_path = _required(record, "ruta_archivo_fuente").strip("/")
    source_name = _required(record, "nombre_archivo_fuente")
    return f"{source_path}/{source_name}" if source_path else source_name


def build_sharepoint_download_url(base_url: str, record: Mapping[str, Any]) -> str:
    if not base_url:
        raise ValueError("base_url SharePoint/Graph es obligatorio")
    reject_signed_or_secret_url(base_url)
    source_file = build_source_file_name(record)
    encoded_path = "/".join(quote(part) for part in source_file.split("/"))
    url = urljoin(base_url.rstrip("/") + "/", encoded_path)
    reject_signed_or_secret_url(url)
    return url


def infer_file_type(record: Mapping[str, Any]) -> str:
    extension = str(record.get("extension_archivo_destino") or record.get("nombre_archivo_fuente") or "").lower()
    if extension.endswith((".xlsx", ".xls")):
        return "excel"
    if extension.endswith(".csv"):
        return "csv"
    return "binary"


def count_rows_if_applicable(content: bytes, file_type: str) -> int | None:
    if file_type != "csv":
        return None
    if not content:
        return 0
    text = content.decode("utf-8-sig", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return 0
    return max(len(lines) - 1, 0)


def metric_record(
    *,
    archivo_origen: str,
    archivo_destino: str,
    bytes_read: int,
    rows_read: int | None,
    status: str,
    error_message: str = "",
) -> dict[str, Any]:
    return {
        "archivo_origen": archivo_origen,
        "archivo_destino": archivo_destino,
        "bytes_read": int(bytes_read),
        "rows_read": rows_read,
        "status": status,
        "error_message": error_message,
        "metric_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


def collect_paginated_graph_items(
    initial_url: str,
    http_get_json: Callable[[str], Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    reject_signed_or_secret_url(initial_url)
    items: list[Mapping[str, Any]] = []
    next_url: str | None = initial_url
    visited: set[str] = set()
    while next_url:
        reject_signed_or_secret_url(next_url)
        if next_url in visited:
            raise ValueError("Paginacion Graph ciclica detectada")
        visited.add(next_url)
        payload = http_get_json(next_url)
        value = payload.get("value", [])
        if isinstance(value, list):
            items.extend(value)
        next_url = payload.get("@odata.nextLink") or payload.get("odata.nextLink")
    return items


def read_sharepoint_secret_values(dbutils: Any, secret_scope: str, secret_names: SharePointSecretNames) -> dict[str, str]:
    if not secret_scope:
        raise ValueError("secret_scope es obligatorio")
    values: dict[str, str] = {}
    for key, secret_name in {
        "token": secret_names.token,
        "client_id": secret_names.client_id,
        "client_secret": secret_names.client_secret,
        "tenant_id": secret_names.tenant_id,
        "base_url": secret_names.base_url,
    }.items():
        if secret_name:
            values[key] = dbutils.secrets.get(secret_scope, secret_name)
    return values


def write_bytes_to_path(spark: Any, path: str, content: bytes, dbutils: Any | None = None) -> None:
    if dbutils is not None:
        suffix = Path(path).suffix
        with NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file.write(content)
            tmp_path = Path(tmp_file.name)
        try:
            dbutils.fs.cp(tmp_path.as_uri(), path, True)
        finally:
            tmp_path.unlink(missing_ok=True)
        return

    hadoop_conf = spark._jsc.hadoopConfiguration()
    j_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hadoop_conf)
    output_stream = fs.create(j_path, True)
    try:
        output_stream.write(bytearray(content))
    finally:
        output_stream.close()


def assert_https_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme.lower() != "https":
        raise ValueError("SharePoint/Graph URL debe usar HTTPS")
