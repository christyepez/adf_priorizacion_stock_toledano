import unittest

from priorizacion_stock_toledano.control.get_control_cargas import (
    CONTROL_VIEW_NAME,
    STANDARD_COLUMNS,
    SqlSecretNames,
    build_get_control_cargas_query,
    build_get_control_cargas_spark_sql,
    filter_by_owner,
    is_exec_statement,
    jdbc_url,
    normalize_control_records,
    read_sql_secret_values,
    validate_active_records,
    validate_control_read_mode,
    validate_inputs,
)


class FakeSecrets:
    def __init__(self):
        self.calls = []

    def get(self, scope, key):
        self.calls.append((scope, key))
        return f"value-{key}"


class FakeDbutils:
    def __init__(self):
        self.secrets = FakeSecrets()


def test_build_get_control_cargas_query_uses_expected_procedure_and_parameters():
    query = build_get_control_cargas_query(
        anio_mes_dia_inicial="20260101",
        anio_mes_dia_final="20260131",
        proceso="Modelo_Priorizacion_Stock",
        sistema_fuente="SapHana",
    )

    assert "EXEC conf.GetControlCargas" in query
    assert "@AñoMesDiaInicial = 20260101" in query
    assert "@AñoMesDiaFinal = 20260131" in query
    assert "@Proceso = N'Modelo_Priorizacion_Stock'" in query
    assert "@SistemaFuente = N'SapHana'" in query


def test_get_control_cargas_query_is_detected_as_exec_statement():
    query = build_get_control_cargas_query(
        anio_mes_dia_inicial="0",
        anio_mes_dia_final="0",
        proceso="Modelo_Priorizacion_Stock",
        sistema_fuente="SapHana",
    )

    assert is_exec_statement(query)
    assert not is_exec_statement("SELECT * FROM conf.ControlCargas")


def test_validate_control_read_mode_accepts_expected_modes():
    assert validate_control_read_mode("jdbc_sp") == "jdbc_sp"
    assert validate_control_read_mode("SPARK_SQL") == "spark_sql"


def test_build_get_control_cargas_spark_sql_builds_select_over_relation():
    query = build_get_control_cargas_spark_sql(
        relation="conf.vw_control_cargas_priorizacion_stock",
        anio_mes_dia_inicial="0",
        anio_mes_dia_final="0",
        proceso="Modelo_Priorizacion_Stock",
        sistema_fuente="SapHana",
    )

    assert "SELECT *" in query
    assert "FROM `conf`.`vw_control_cargas_priorizacion_stock`" in query
    assert "WHERE" not in query


def test_normalize_control_records_preserves_arm_compatibility():
    records = [
        {
            "Proceso": "Modelo_Priorizacion_Stock",
            "SistemaFuente": "SapHana",
            "PropietarioFuente": "VistasSapHana",
            "ColumnasArchivoFuente": "COL1,COL2",
            "RutaArchivoFuente": "SCHEMA",
            "NombreArchivoFuente": "VIEW_NAME",
            "FiltrosArchivoFuente": "WHERE 1=1",
            "RutaArchivoDestino": "sap/path",
            "NombreArchivoDestino": "file_name",
            "ExtencionArchivoDestino": ".parquet",
            "TipoCarga": "full",
            "Activo": 1,
            "OrdenEjecucion": "10",
        }
    ]

    normalized = normalize_control_records(records)

    assert list(normalized[0].keys()) == STANDARD_COLUMNS
    assert normalized[0]["proceso"] == "Modelo_Priorizacion_Stock"
    assert normalized[0]["sistema_fuente"] == "SapHana"
    assert normalized[0]["propietario_fuente"] == "VistasSapHana"
    assert normalized[0]["extension_archivo_destino"] == ".parquet"
    assert normalized[0]["activo"] is True
    assert normalized[0]["orden_ejecucion"] == 10


def test_filter_by_owner_accepts_expected_owners():
    records = normalize_control_records(
        [
            {"PropietarioFuente": "VistasSapHana", "Activo": 1},
            {"PropietarioFuente": "DatosPortalDeInformacion", "Activo": 1},
        ]
    )

    filtered = filter_by_owner(records, "DatosPortalDeInformacion")

    assert len(filtered) == 1
    assert filtered[0]["propietario_fuente"] == "DatosPortalDeInformacion"


def test_validate_active_records_rejects_empty_active_result():
    records = normalize_control_records([{"PropietarioFuente": "VistasSapHana", "Activo": 0}])

    with unittest.TestCase().assertRaises(ValueError):
        validate_active_records(records)


def test_validate_inputs_rejects_invalid_sistema_fuente():
    with unittest.TestCase().assertRaises(ValueError):
        validate_inputs("Modelo_Priorizacion_Stock", "otro")


def test_jdbc_url_does_not_include_credentials():
    url = jdbc_url("sql.example.local", "control")

    assert "jdbc:sqlserver://sql.example.local;" in url
    assert "databaseName=control" in url
    assert "password" not in url.lower()
    assert "user" not in url.lower()


def test_control_view_name_contract():
    assert CONTROL_VIEW_NAME == "vw_control_cargas_priorizacion_stock"


def test_read_sql_secret_values_allows_non_sensitive_server_and_database_values():
    dbutils = FakeDbutils()

    values = read_sql_secret_values(
        dbutils,
        "scope",
        SqlSecretNames(
            server="server-secret",
            database="database-secret",
            username="username-secret",
            password="password-secret",
            server_value="sql-control.local",
            database_value="ControlDB",
        ),
    )

    assert values["server"] == "sql-control.local"
    assert values["database"] == "ControlDB"
    assert values["username"] == "value-username-secret"
    assert values["password"] == "value-password-secret"
    assert ("scope", "server-secret") not in dbutils.secrets.calls
    assert ("scope", "database-secret") not in dbutils.secrets.calls
