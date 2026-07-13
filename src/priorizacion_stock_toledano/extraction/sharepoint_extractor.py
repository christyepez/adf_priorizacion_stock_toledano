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
SHAREPOINT_METRICS_COLUMNS = [
    "archivo_origen",
    "archivo_destino",
    "bytes_read",
    "rows_read",
    "status",
    "error_message",
    "metric_timestamp_utc",
]
FILE_PATH_AUTH_MODES = {"databricks_path", "databricks_volume", "mounted_path", "external_location"}


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


def is_sharepoint_host(url: str) -> bool:
    parsed = urlparse(url or "")
    return parsed.hostname is not None and parsed.hostname.lower().endswith(".sharepoint.com")


def oauth_scope_for_sharepoint(base_url: str, auth_mode: str) -> str:
    parsed = urlparse(base_url or "")
    mode = (auth_mode or "").strip().lower()
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("sharepoint_base_url debe ser una URL absoluta HTTPS")

    if mode in {"graph_client_credentials", "graph"}:
        return "https://graph.microsoft.com/.default"

    if is_sharepoint_host(base_url):
        return f"{parsed.scheme}://{parsed.netloc}/.default"

    if mode in {"sharepoint_client_credentials", "sharepoint"}:
        return f"{parsed.scheme}://{parsed.netloc}/.default"

    return "https://graph.microsoft.com/.default"


def build_sharepoint_rest_download_url(base_url: str, record: Mapping[str, Any]) -> str:
    parsed = urlparse(base_url or "")
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("base_url SharePoint es obligatorio")
    source_file = build_source_file_name(record).strip("/")
    base_path = parsed.path.strip("/")
    if base_path and not source_file.lower().startswith(base_path.lower() + "/"):
        source_file = f"{base_path}/{source_file}"
    server_relative_path = f"/{source_file}"
    encoded_server_relative_path = quote(server_relative_path, safe="/")
    url = f"{parsed.scheme}://{parsed.netloc}/_api/web/GetFileByServerRelativeUrl('{encoded_server_relative_path}')/$value"
    reject_signed_or_secret_url(url)
    return url


def build_sharepoint_download_url(base_url: str, record: Mapping[str, Any]) -> str:
    if not base_url:
        raise ValueError("base_url SharePoint/Graph es obligatorio")
    reject_signed_or_secret_url(base_url)
    if is_sharepoint_host(base_url):
        return build_sharepoint_rest_download_url(base_url, record)
    source_file = build_source_file_name(record)
    encoded_path = "/".join(quote(part) for part in source_file.split("/"))
    url = urljoin(base_url.rstrip("/") + "/", encoded_path)
    reject_signed_or_secret_url(url)
    return url


def is_databricks_file_path_mode(auth_mode: str) -> bool:
    return (auth_mode or "").strip().lower() in FILE_PATH_AUTH_MODES


def build_databricks_source_path(base_path: str, record: Mapping[str, Any]) -> str:
    clean_base = (base_path or "").strip().rstrip("/")
    if not clean_base:
        raise ValueError("sharepoint_connection_path es obligatorio para modo databricks_path")
    source_file = build_source_file_name(record).strip("/")
    if not source_file:
        raise ValueError("Ruta de archivo SharePoint vacia")
    return f"{clean_base}/{source_file}"


def graph_site_file_candidates(base_url: str, record: Mapping[str, Any]) -> list[tuple[str, str]]:
    parsed = urlparse(base_url or "")
    if not parsed.hostname:
        raise ValueError("sharepoint_base_url debe incluir host para usar Graph")

    source_file = build_source_file_name(record).strip("/")
    source_parts = [part for part in source_file.split("/") if part]
    base_parts = [part for part in parsed.path.strip("/").split("/") if part]
    candidates: list[tuple[str, str]] = []

    if source_file:
        candidates.append(("/", source_file))

    if base_parts:
        site_path = "/" + "/".join(base_parts)
        file_path = source_file
        if source_file.lower().startswith("/".join(base_parts).lower() + "/"):
            file_path = "/".join(source_parts[len(base_parts) :])
        candidates.append((site_path, file_path))

    if source_parts:
        site_name = source_parts[0]
        file_path = "/".join(source_parts[1:])
        candidates.append((f"/sites/{site_name}", file_path))
        candidates.append((f"/{site_name}", file_path))

    unique_candidates: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for site_path, file_path in candidates:
        normalized = (site_path.strip(), file_path.strip("/"))
        if normalized[0] and normalized[1] and normalized not in seen:
            unique_candidates.append(normalized)
            seen.add(normalized)
    return unique_candidates


def _graph_site_lookup_url(hostname: str, site_path: str) -> str:
    encoded_site_path = quote(site_path, safe="/")
    return f"https://graph.microsoft.com/v1.0/sites/{hostname}:{encoded_site_path}"


def _graph_file_content_url(site_id: str, file_path: str) -> str:
    encoded_file_path = quote(file_path.strip("/"), safe="/")
    return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{encoded_file_path}:/content"


def _graph_drives_url(site_id: str) -> str:
    return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"


def _graph_drive_file_content_url(drive_id: str, file_path: str) -> str:
    encoded_file_path = quote(file_path.strip("/"), safe="/")
    return f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}:/content"


def graph_drive_file_candidates(record: Mapping[str, Any]) -> list[tuple[str, str]]:
    source_file = build_source_file_name(record).strip("/")
    source_parts = [part for part in source_file.split("/") if part]
    if len(source_parts) < 2:
        return []
    drive_name = source_parts[0]
    file_path = "/".join(source_parts[1:])
    return [(drive_name, file_path)]


def _drive_matches_name(drive: Mapping[str, Any], expected_name: str) -> bool:
    expected = (expected_name or "").strip().lower()
    if not expected:
        return False
    name = str(drive.get("name") or "").strip().lower()
    web_url = str(drive.get("webUrl") or "").strip().lower()
    return name == expected or web_url.rstrip("/").endswith("/" + expected)


def download_sharepoint_content(
    *,
    base_url: str,
    record: Mapping[str, Any],
    headers: Mapping[str, str],
    http_get: Callable[..., Any],
    auth_mode: str = "graph_client_credentials",
    timeout: int = 300,
) -> bytes:
    mode = (auth_mode or "").strip().lower()
    if mode in {"graph_client_credentials", "graph"}:
        parsed = urlparse(base_url or "")
        hostname = parsed.hostname
        if not hostname:
            raise ValueError("sharepoint_base_url debe incluir host para usar Graph")
        errors: list[str] = []
        resolved_sites: list[tuple[str, str]] = []
        for site_path, file_path in graph_site_file_candidates(base_url, record):
            site_url = _graph_site_lookup_url(hostname, site_path)
            try:
                site_response = http_get(site_url, headers=headers, timeout=timeout)
                site_response.raise_for_status()
                site_id = site_response.json()["id"]
                resolved_sites.append((site_path, site_id))
                content_url = _graph_file_content_url(site_id, file_path)
                content_response = http_get(content_url, headers=headers, timeout=timeout)
                content_response.raise_for_status()
                return content_response.content
            except Exception as exc:
                errors.append(f"{site_path}/{file_path}: {exc}")

        for site_path, site_id in resolved_sites:
            for drive_name, file_path in graph_drive_file_candidates(record):
                try:
                    drives_response = http_get(_graph_drives_url(site_id), headers=headers, timeout=timeout)
                    drives_response.raise_for_status()
                    drives = drives_response.json().get("value", [])
                    matching_drives = [drive for drive in drives if _drive_matches_name(drive, drive_name)]
                    if not matching_drives:
                        raise RuntimeError(f"No existe drive/biblioteca {drive_name!r} en el sitio {site_path}")
                    for drive in matching_drives:
                        drive_id = drive["id"]
                        content_response = http_get(
                            _graph_drive_file_content_url(drive_id, file_path),
                            headers=headers,
                            timeout=timeout,
                        )
                        content_response.raise_for_status()
                        return content_response.content
                except Exception as exc:
                    errors.append(f"{site_path} drive {drive_name}/{file_path}: {exc}")

        raise RuntimeError(
            "No se pudo descargar el archivo desde SharePoint usando Microsoft Graph. "
            "Valida permisos Graph del App Registration y la ruta del archivo. Intentos: "
            + " | ".join(errors)
        )

    download_url = build_sharepoint_download_url(base_url, record)
    response = http_get(download_url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.content


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


def sharepoint_metrics_schema() -> Any:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("archivo_origen", StringType(), True),
            StructField("archivo_destino", StringType(), True),
            StructField("bytes_read", LongType(), True),
            StructField("rows_read", LongType(), True),
            StructField("status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("metric_timestamp_utc", StringType(), True),
        ]
    )


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
