import unittest

from priorizacion_stock_toledano.extraction.sharepoint_extractor import (
    build_bronze_path,
    build_sharepoint_download_url,
    build_source_file_name,
    collect_paginated_graph_items,
    count_rows_if_applicable,
    infer_file_type,
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
