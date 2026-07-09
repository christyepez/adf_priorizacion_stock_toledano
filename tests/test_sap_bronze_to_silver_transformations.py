import unittest

from priorizacion_stock_toledano.transformations.sap_bronze_to_silver import (
    CV_LO_PEDIDO_CASTS,
    M_OFERTAS_DOC_VENTAS_CASTS,
    clean_null_value,
    remove_accents_value,
    required_columns,
    silver_table_path,
    validate_required_columns,
)


def test_required_columns_preserve_m_ofertas_order():
    assert required_columns(M_OFERTAS_DOC_VENTAS_CASTS) == [
        "DATE_SQL",
        "ERDAT",
        "ERZET",
        "FKDAT",
        "KWMENG",
        "Cantidad_Pedida",
        "KUNNR",
        "IdMaterial",
        "BRGEW",
    ]


def test_required_columns_preserve_cv_lo_pedido_order():
    assert required_columns(CV_LO_PEDIDO_CASTS) == [
        "MATNR",
        "WERKS",
        "DATE_SQL",
        "EBELN",
        "VBELN",
        "Cantidad_Pedida",
        "Peso_Pedido",
        "PesoLBS_Pedido",
        "Volumen_Pedido_LBS_DOC",
    ]


def test_validate_required_columns_raises_for_missing_column():
    with unittest.TestCase().assertRaises(ValueError):
        validate_required_columns(["DATE_SQL"], ["DATE_SQL", "ERDAT"])


def test_remove_accents_value_matches_nb_utils_equivalent():
    assert remove_accents_value("Priorización Ñandú") == "Priorizacion Nandu"


def test_clean_null_value_normalizes_common_null_strings():
    assert clean_null_value(" NULL ") is None
    assert clean_null_value("n/a") is None
    assert clean_null_value(" value ") == "value"


def test_silver_table_path_uses_storage_account_variable():
    assert (
        silver_table_path("dlsbigdatatoledanodev", "sap/fact_cv_lo_pedido")
        == "abfss://silver@dlsbigdatatoledanodev.dfs.core.windows.net/sap/fact_cv_lo_pedido"
    )
