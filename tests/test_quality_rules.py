from priorizacion_stock_toledano.quality.quality_rules import (
    build_default_quality_tables,
    build_quality_result,
    quality_results_table_identifier,
)
from priorizacion_stock_toledano.quality.reconciliation import compare_counts, run_reconciliation


class FakeCatalog:
    def __init__(self, existing):
        self.existing = set(existing)

    def tableExists(self, table_name):
        return table_name in self.existing


class FakeDataFrame:
    def __init__(self, count):
        self._count = count

    def count(self):
        return self._count


class FakeSpark:
    def __init__(self, counts):
        self.counts = counts
        self.catalog = FakeCatalog(counts.keys())

    def table(self, table_name):
        return FakeDataFrame(self.counts[table_name])


def test_quality_results_table_identifier_defaults_to_gold_atlas():
    assert quality_results_table_identifier("toledano_gold_dev", "atlas") == "toledano_gold_dev.atlas.quality_results"


def test_build_default_quality_tables_uses_expected_model_tables():
    tables = build_default_quality_tables(
        catalog_bronze="bronze",
        catalog_silver="silver",
        catalog_gold="gold",
        schema_sap="sap",
        schema_sharepoint="sharepoint",
        schema_atlas="atlas",
    )

    assert "bronze.sap.fact_cv_m_ofertas_doc_ventas" in tables["bronze_sap"]
    assert "bronze.sharepoint.priorizaciones_previas" in tables["bronze_sharepoint"]
    assert "silver.sap.fact_cv_lo_pedido" in tables["silver"]
    assert tables["gold"] == "gold.atlas.resultados_indice_priorizacion"


def test_build_quality_result_has_required_columns_and_failed_status():
    result = build_quality_result(
        execution_id="run-1",
        ambiente="dev",
        proceso="Modelo_Priorizacion_Stock",
        tabla="gold.atlas.resultados_indice_priorizacion",
        regla="gold_has_data",
        passed=False,
        cantidad_errores=1,
        severidad="CRITICAL",
        mensaje="Sin datos",
        timestamp="2026-07-09T20:00:00+00:00",
    )

    assert result.resultado == "FAILED"
    assert result.as_dict()["cantidad_errores"] == 1
    assert set(result.as_dict()) == {
        "execution_id",
        "ambiente",
        "proceso",
        "tabla",
        "regla",
        "resultado",
        "cantidad_errores",
        "severidad",
        "mensaje",
        "timestamp",
    }


def test_compare_counts_respects_tolerance_pct():
    passed, difference, message = compare_counts(100, 103, tolerance_pct=0.05)

    assert passed is True
    assert difference == 3
    assert "ADF=100" in message


def test_run_reconciliation_skips_when_no_adf_source():
    spark = FakeSpark({"gold.atlas.resultados_indice_priorizacion": 10})

    results = run_reconciliation(
        spark,
        execution_id="run-1",
        ambiente="dev",
        proceso="Modelo_Priorizacion_Stock",
        databricks_table="gold.atlas.resultados_indice_priorizacion",
        adf_table=None,
    )

    assert results[0].resultado == "PASSED"
    assert results[0].severidad == "WARNING"
    assert "omitida" in results[0].mensaje


def test_run_reconciliation_fails_on_count_difference_without_tolerance():
    spark = FakeSpark(
        {
            "adf.resultados_indice_priorizacion": 10,
            "gold.atlas.resultados_indice_priorizacion": 8,
        }
    )

    results = run_reconciliation(
        spark,
        execution_id="run-1",
        ambiente="dev",
        proceso="Modelo_Priorizacion_Stock",
        databricks_table="gold.atlas.resultados_indice_priorizacion",
        adf_table="adf.resultados_indice_priorizacion",
    )

    assert results[0].resultado == "FAILED"
    assert results[0].cantidad_errores == 2
