import unittest

from priorizacion_stock_toledano.transformations.sharepoint_bronze_to_silver import (
    GRUPOS_PRIORIZACION_CASTS,
    GRUPOS_PRIORIZACION_RENAME_MAP,
    PRIORIZACIONES_PREVIAS_CASTS,
    PRIORIZACIONES_PREVIAS_RENAME_MAP,
    expected_output_columns,
    required_source_columns,
)
from priorizacion_stock_toledano.transformations.sap_bronze_to_silver import (
    clean_null_value,
    remove_accents_value,
    validate_required_columns,
)


def test_grupos_priorizacion_source_columns_match_original_notebook():
    assert required_source_columns(GRUPOS_PRIORIZACION_RENAME_MAP) == [
        "Cliente",
        "Nombre",
        "CADENA",
        "GRUPO",
        "Priorización",
    ]


def test_grupos_priorizacion_output_columns_match_expected_schema():
    assert expected_output_columns(GRUPOS_PRIORIZACION_CASTS) == [
        "cliente",
        "nombre",
        "cadena",
        "grupo",
        "priorizacion",
    ]


def test_priorizaciones_previas_source_columns_match_original_notebook():
    assert required_source_columns(PRIORIZACIONES_PREVIAS_RENAME_MAP) == [
        "CODIGO",
        "CLIENTE",
        "PRIORIZACIÓN",
    ]


def test_priorizaciones_previas_output_columns_match_expected_schema():
    assert expected_output_columns(PRIORIZACIONES_PREVIAS_CASTS) == [
        "codigo",
        "cliente",
        "priorizacion",
    ]


def test_casts_match_original_logic():
    assert GRUPOS_PRIORIZACION_CASTS["cliente"] == "int"
    assert GRUPOS_PRIORIZACION_CASTS["priorizacion"] == "int"
    assert PRIORIZACIONES_PREVIAS_CASTS["codigo"] == "string"
    assert PRIORIZACIONES_PREVIAS_CASTS["priorizacion"] == "int"


def test_shared_cleaning_helpers_keep_tildes_and_null_logic():
    assert remove_accents_value("PRIORIZACIÓN") == "PRIORIZACION"
    assert clean_null_value(" nan ") is None


def test_missing_sharepoint_column_fails_fast():
    with unittest.TestCase().assertRaises(ValueError):
        validate_required_columns(["Cliente"], required_source_columns(GRUPOS_PRIORIZACION_RENAME_MAP))
