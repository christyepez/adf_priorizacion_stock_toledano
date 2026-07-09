import unittest

from priorizacion_stock_toledano.model.model_parameters import TABLE_KEYS, obtener_tablas, validar_ambiente


def test_obtener_tablas_dev_defaults_match_industrialized_contract():
    tablas = obtener_tablas("dev")

    assert tablas == {
        "TBL_RENTABILIDAD_SKU": "toledano_silver_dev.sap.fact_fi_rentabilidad_sku",
        "TBL_COSTO_SERVIR": "toledano_silver_dev.sap.fact_fi_gastos_cs_cliente",
        "TBL_DIM_MATERIALES": "toledano_silver_dev.sap.dim_materiales",
        "TBL_DIM_CLIENTES": "toledano_silver_dev.sap.dim_clientes_direccion",
        "TBL_CLIENTES_PRIORIZADOS": "toledano_silver_dev.sharepoint.grupos_priorizacion",
        "TBL_INDICE_PRIORIZACION_ANTERIOR": "toledano_silver_dev.sharepoint.priorizaciones_previas",
        "TBL_OUTPUT_INDICE_PRIORIZACION": "toledano_gold_dev.atlas.resultados_indice_priorizacion",
        "TBL_OUTPUT_INDICE_PRIORIZACION_HISTORICO": "toledano_gold_dev.atlas.resultados_indice_priorizacion_hist",
    }


def test_obtener_tablas_prd_defaults_match_expected_prd_catalogs():
    tablas = obtener_tablas("prd")

    assert tablas["TBL_RENTABILIDAD_SKU"] == "toledano_silver_prd.sap.fact_fi_rentabilidad_sku"
    assert tablas["TBL_OUTPUT_INDICE_PRIORIZACION"] == "toledano_gold_prd.atlas.resultados_indice_priorizacion"


def test_obtener_tablas_allows_bundle_overrides():
    tablas = obtener_tablas(
        "test",
        catalog_silver="custom_silver",
        catalog_gold="custom_gold",
        schema_sap="sap_curated",
        schema_sharepoint="portal",
        schema_atlas="modelos",
    )

    assert tablas["TBL_COSTO_SERVIR"] == "custom_silver.sap_curated.fact_fi_gastos_cs_cliente"
    assert tablas["TBL_CLIENTES_PRIORIZADOS"] == "custom_silver.portal.grupos_priorizacion"
    assert tablas["TBL_OUTPUT_INDICE_PRIORIZACION_HISTORICO"] == "custom_gold.modelos.resultados_indice_priorizacion_hist"


def test_obtener_tablas_returns_all_required_keys_in_order():
    tablas = obtener_tablas("dev")

    assert list(tablas.keys()) == TABLE_KEYS


def test_validar_ambiente_accepts_dev_test_prd():
    assert validar_ambiente("dev") == "dev"
    assert validar_ambiente("test") == "test"
    assert validar_ambiente("prd") == "prd"


def test_validar_ambiente_rejects_unknown_environment():
    with unittest.TestCase().assertRaises(ValueError):
        validar_ambiente("sandbox")
