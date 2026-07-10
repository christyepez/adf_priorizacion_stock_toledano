import unittest

from priorizacion_stock_toledano.extraction.saphana_extractor import (
    build_bronze_path,
    build_saphana_query,
    choose_output_format,
    hana_jdbc_url,
)


def test_build_saphana_query_with_filters():
    record = {
        "columnas_archivo_fuente": "COL1, COL2",
        "ruta_archivo_fuente": "SCHEMA",
        "nombre_archivo_fuente": "VIEW_NAME",
        "filtros_archivo_fuente": "WHERE COL1 >= 1",
    }

    query = build_saphana_query(record)

    assert query == 'SELECT COL1, COL2\nFROM "SCHEMA"."VIEW_NAME"\nWHERE COL1 >= 1'


def test_build_saphana_query_without_filters():
    record = {
        "columnas_archivo_fuente": "*",
        "ruta_archivo_fuente": "VistasSapHana",
        "nombre_archivo_fuente": "CV_LO_PEDIDO",
        "filtros_archivo_fuente": "",
    }

    query = build_saphana_query(record)

    assert query == 'SELECT *\nFROM "VistasSapHana"."CV_LO_PEDIDO"'


def test_build_saphana_query_escapes_double_quotes_in_identifier():
    record = {
        "columnas_archivo_fuente": "COL1",
        "ruta_archivo_fuente": 'SCHEMA"A',
        "nombre_archivo_fuente": 'VIEW"B',
    }

    query = build_saphana_query(record)

    assert query == 'SELECT COL1\nFROM "SCHEMA""A"."VIEW""B"'


def test_build_saphana_query_rejects_semicolon_in_filter():
    record = {
        "columnas_archivo_fuente": "COL1",
        "ruta_archivo_fuente": "SCHEMA",
        "nombre_archivo_fuente": "VIEW_NAME",
        "filtros_archivo_fuente": "WHERE 1=1; DROP TABLE X",
    }

    with unittest.TestCase().assertRaises(ValueError):
        build_saphana_query(record)


def test_build_bronze_path_uses_adls_bronze_container():
    path = build_bronze_path(
        storage_account="dlsbigdatatoledanodev",
        folder="sap/cv_lo_pedido",
        filename="cv_lo_pedido",
        extension=".parquet",
    )

    assert path == "abfss://bronze@dlsbigdatatoledanodev.dfs.core.windows.net/sap/cv_lo_pedido/cv_lo_pedido.parquet"


def test_choose_output_format_prefers_parquet_when_control_indicates_parquet():
    assert choose_output_format({"extension_archivo_destino": ".parquet"}) == "parquet"


def test_choose_output_format_defaults_to_delta():
    assert choose_output_format({"extension_archivo_destino": ".csv"}) == "delta"


def test_hana_jdbc_url_uses_default_port_and_no_credentials():
    url = hana_jdbc_url("hana.example.local")

    assert url == "jdbc:sap://hana.example.local:30015"
    assert "password" not in url.lower()
    assert "user" not in url.lower()
