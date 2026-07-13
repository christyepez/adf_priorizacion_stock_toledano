import unittest

from priorizacion_stock_toledano.extraction.sharepoint_extractor import (
    SHAREPOINT_METRICS_COLUMNS,
    build_bronze_path,
    build_databricks_source_path,
    build_sharepoint_download_url,
    build_source_file_name,
    collect_paginated_graph_items,
    count_rows_if_applicable,
    download_sharepoint_content,
    graph_drive_file_candidates,
    graph_site_file_candidates,
    infer_file_type,
    is_absolute_databricks_path,
    is_databricks_file_path_mode,
    oauth_scope_for_sharepoint,
    reject_signed_or_secret_url,
)


def test_build_source_file_name_from_control_record():
    record = {
        "ruta_archivo_fuente": "sites/portal/documentos",
        "nombre_archivo_fuente": "priorizaciones.xlsx",
    }

    assert build_source_file_name(record) == "sites/portal/documentos/priorizaciones.xlsx"


def test_build_sharepoint_download_url_encodes_spaces():
    record = {
        "ruta_archivo_fuente": "sites/portal/documentos compartidos",
        "nombre_archivo_fuente": "grupos priorizacion.xlsx",
    }

    url = build_sharepoint_download_url("https://graph.microsoft.com/v1.0/", record)

    assert url == "https://graph.microsoft.com/v1.0/sites/portal/documentos%20compartidos/grupos%20priorizacion.xlsx"


def test_build_sharepoint_download_url_uses_rest_api_for_sharepoint_host():
    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    url = build_sharepoint_download_url("https://pronaca365.sharepoint.com/", record)

    assert (
        url
        == "https://pronaca365.sharepoint.com/_api/web/GetFileByServerRelativeUrl('/Toledano/asignacion_stock/Grupos%20Priorizaci%C3%B3n.xlsx')/$value"
    )


def test_build_databricks_source_path_uses_connection_base_path():
    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    path = build_databricks_source_path("/Volumes/ext/sharepoint/raw", record)

    assert path == "/Volumes/ext/sharepoint/raw/Toledano/asignacion_stock/Grupos Priorización.xlsx"


def test_build_databricks_source_path_keeps_absolute_control_path():
    record = {
        "ruta_archivo_fuente": "/Volumes/ext/sharepoint/raw/Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    path = build_databricks_source_path("", record)

    assert path == "/Volumes/ext/sharepoint/raw/Toledano/asignacion_stock/Grupos Priorización.xlsx"


def test_databricks_path_auth_mode_is_supported():
    assert is_databricks_file_path_mode("databricks_path")
    assert is_databricks_file_path_mode("databricks_volume")
    assert not is_databricks_file_path_mode("sharepoint_client_credentials")


def test_absolute_databricks_path_detection():
    assert is_absolute_databricks_path("/Volumes/ext/sharepoint/raw/file.xlsx")
    assert is_absolute_databricks_path("dbfs:/mnt/sharepoint/file.xlsx")
    assert is_absolute_databricks_path("abfss://raw@storage.dfs.core.windows.net/file.xlsx")
    assert not is_absolute_databricks_path("Toledano/asignacion_stock/file.xlsx")


def test_oauth_scope_uses_sharepoint_resource_for_sharepoint_host():
    scope = oauth_scope_for_sharepoint("https://pronaca365.sharepoint.com/", "sharepoint_client_credentials")

    assert scope == "https://pronaca365.sharepoint.com/.default"


def test_oauth_scope_uses_graph_resource_for_graph_auth_mode():
    scope = oauth_scope_for_sharepoint("https://pronaca365.sharepoint.com/", "graph_client_credentials")

    assert scope == "https://graph.microsoft.com/.default"


def test_download_sharepoint_content_uses_direct_sharepoint_rest():
    class Response:
        content = b"file-content"

        def raise_for_status(self):
            return None

    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return Response()

    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    content = download_sharepoint_content(
        base_url="https://pronaca365.sharepoint.com/",
        record=record,
        headers={"Authorization": "Bearer token"},
        http_get=fake_get,
        auth_mode="sharepoint_client_credentials",
    )

    assert content == b"file-content"
    assert calls == [
        "https://pronaca365.sharepoint.com/_api/web/GetFileByServerRelativeUrl('/Toledano/asignacion_stock/Grupos%20Priorizaci%C3%B3n.xlsx')/$value"
    ]


def test_graph_site_file_candidates_from_control_path():
    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    assert graph_site_file_candidates("https://pronaca365.sharepoint.com/", record) == [
        ("/", "Toledano/asignacion_stock/Grupos Priorización.xlsx"),
        ("/sites/Toledano", "asignacion_stock/Grupos Priorización.xlsx"),
        ("/Toledano", "asignacion_stock/Grupos Priorización.xlsx"),
    ]


def test_graph_drive_file_candidates_from_document_library_path():
    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    assert graph_drive_file_candidates(record) == [
        ("Toledano", "asignacion_stock/Grupos Priorización.xlsx")
    ]


def test_download_sharepoint_content_uses_graph_candidates():
    class Response:
        def __init__(self, *, payload=None, content=b"", status_code=200):
            self._payload = payload or {}
            self.content = content
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"{self.status_code} error")

        def json(self):
            return self._payload

    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if "/sites/pronaca365.sharepoint.com:/" in url:
            return Response(payload={"id": "site-id"})
        if "/sites/site-id/drive/root:/Toledano/asignacion_stock/Grupos%20Priorizaci%C3%B3n.xlsx:/content" in url:
            return Response(content=b"file-content")
        return Response(status_code=404)

    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    content = download_sharepoint_content(
        base_url="https://pronaca365.sharepoint.com/",
        record=record,
        headers={"Authorization": "Bearer token"},
        http_get=fake_get,
        auth_mode="graph_client_credentials",
    )

    assert content == b"file-content"
    assert len(calls) == 2


def test_download_sharepoint_content_falls_back_to_matching_drive():
    class Response:
        def __init__(self, *, payload=None, content=b"", status_code=200):
            self._payload = payload or {}
            self.content = content
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"{self.status_code} error")

        def json(self):
            return self._payload

    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if "/sites/pronaca365.sharepoint.com:/" in url:
            return Response(payload={"id": "site-id"})
        if "/sites/site-id/drive/root:/Toledano/asignacion_stock/Grupos%20Priorizaci%C3%B3n.xlsx:/content" in url:
            return Response(status_code=404)
        if "/sites/site-id/drives" in url:
            return Response(payload={"value": [{"id": "drive-id", "name": "Toledano"}]})
        if "/drives/drive-id/root:/asignacion_stock/Grupos%20Priorizaci%C3%B3n.xlsx:/content" in url:
            return Response(content=b"file-content")
        return Response(status_code=404)

    record = {
        "ruta_archivo_fuente": "Toledano/asignacion_stock",
        "nombre_archivo_fuente": "Grupos Priorización.xlsx",
    }

    content = download_sharepoint_content(
        base_url="https://pronaca365.sharepoint.com/",
        record=record,
        headers={"Authorization": "Bearer token"},
        http_get=fake_get,
        auth_mode="graph_client_credentials",
    )

    assert content == b"file-content"
    assert any("/sites/site-id/drives" in call for call in calls)


def test_reject_signed_or_secret_url_blocks_sas_like_url():
    with unittest.TestCase().assertRaises(ValueError):
        reject_signed_or_secret_url("https://example.test/file.xlsx?sig=abc")


def test_build_bronze_path_uses_control_destination():
    path = build_bronze_path(
        storage_account="dlsbigdatatoledanodev",
        folder="sharepoint/datos_portal/grupos",
        filename="grupos_priorizacion",
        extension="xlsx",
    )

    assert path == "abfss://bronze@dlsbigdatatoledanodev.dfs.core.windows.net/sharepoint/datos_portal/grupos/grupos_priorizacion.xlsx"


def test_infer_file_type_supports_excel_csv_and_binary():
    assert infer_file_type({"extension_archivo_destino": ".xlsx"}) == "excel"
    assert infer_file_type({"extension_archivo_destino": ".csv"}) == "csv"
    assert infer_file_type({"extension_archivo_destino": ".pdf"}) == "binary"


def test_count_rows_if_applicable_for_csv_ignores_header():
    content = b"col1,col2\n1,2\n3,4\n"

    assert count_rows_if_applicable(content, "csv") == 2


def test_count_rows_if_applicable_returns_none_for_excel():
    assert count_rows_if_applicable(b"binary", "excel") is None


def test_collect_paginated_graph_items_follows_next_link():
    pages = {
        "https://graph.test/page1": {"value": [{"id": "1"}], "@odata.nextLink": "https://graph.test/page2?$skiptoken=abc"},
        "https://graph.test/page2?$skiptoken=abc": {"value": [{"id": "2"}]},
    }

    items = collect_paginated_graph_items("https://graph.test/page1", lambda url: pages[url])

    assert items == [{"id": "1"}, {"id": "2"}]


def test_sharepoint_metrics_columns_are_stable():
    assert SHAREPOINT_METRICS_COLUMNS == [
        "archivo_origen",
        "archivo_destino",
        "bytes_read",
        "rows_read",
        "status",
        "error_message",
        "metric_timestamp_utc",
    ]
