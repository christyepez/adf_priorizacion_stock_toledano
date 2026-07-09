from priorizacion_stock_toledano.model import indice_priorizacion as indice


class FakeGroupedData:
    def __init__(self, parent, grouped_by):
        self.parent = parent
        self.grouped_by = grouped_by

    def agg(self, expression):
        self.parent.last_grouped_by = self.grouped_by
        self.parent.last_agg_expression = expression
        return self.parent


class FakeDataFrame:
    def __init__(self, columns=None):
        self.columns = columns or []
        self.last_grouped_by = None
        self.last_agg_expression = None

    def groupBy(self, grouped_by):
        return FakeGroupedData(self, grouped_by)


def test_validate_input_tables_requires_all_keys():
    tablas = {key: f"catalog.schema.{key.lower()}" for key in indice.REQUIRED_TABLE_KEYS}
    tablas.pop("TBL_COSTO_SERVIR")

    try:
        indice.validate_input_tables(tablas)
    except ValueError as exc:
        assert "TBL_COSTO_SERVIR" in str(exc)
    else:
        raise AssertionError("validate_input_tables debio rechazar tablas incompletas")


def test_require_columns_reports_missing_columns():
    df = FakeDataFrame(columns=["cod_cliente", "fecha"])

    try:
        indice.require_columns(df, ["cod_cliente", "fecha", "valor_neto"], "RENTABILIDAD")
    except ValueError as exc:
        assert "valor_neto" in str(exc)
        assert "RENTABILIDAD" in str(exc)
    else:
        raise AssertionError("require_columns debio rechazar columnas faltantes")


def test_metric_columns_present_supports_optional_original_columns():
    presence = indice.metric_columns_present(
        ["margen_bruto_cliente_sum", "distancia_cliente"],
        col_margen="margen_bruto_cliente_sum",
        col_estabilidad="cv_margen_bruto_cliente",
        col_distancia="distancia_cliente",
        col_margen_prom="margen_bruto_prom_cliente",
    )

    assert presence == {
        "margen": True,
        "estabilidad": False,
        "distancia": True,
        "margen_prom": False,
    }


def test_calcular_margen_acumulado_groups_and_aliases_sum(monkeypatch):
    df = FakeDataFrame(columns=["cod_cliente", "margen_bruto"])
    monkeypatch.setattr(indice, "_sum_column", lambda column, alias: {"sum": column, "alias": alias})

    result = indice.calcular_margen_acumulado(df, "cod_cliente", "margen_bruto", "margen_bruto_sum")

    assert result is df
    assert df.last_grouped_by == "cod_cliente"
    assert df.last_agg_expression == {"sum": "margen_bruto", "alias": "margen_bruto_sum"}


def test_model_constants_preserve_original_business_parameters():
    assert indice.DEFAULT_NUM_MESES == 12
    assert indice.DEFAULT_COD_CENTROS == ["3900", "3903", "3905", "3904"]
    assert indice.EARTH_RADIUS_KM == 6371.0
