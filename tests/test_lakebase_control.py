from priorizacion_stock_toledano.control.lakebase_control import (
    LAKEBASE_CONTROL_VIEW,
    LakebaseSecretNames,
    build_lakebase_control_query,
    lakebase_jdbc_url,
    read_lakebase_secret_values,
    validate_lakebase_inputs,
)


class FakeSecrets:
    def __init__(self):
        self.calls = []
        self.values = {
            ("scope", "host"): "lakebase.example.databricks.com",
            ("scope", "port"): "5432",
            ("scope", "database"): "control",
            ("scope", "username"): "user",
            ("scope", "password"): "pwd",
        }

    def get(self, scope, key):
        self.calls.append((scope, key))
        return self.values[(scope, key)]


class FakeDbutils:
    def __init__(self):
        self.secrets = FakeSecrets()


def test_lakebase_jdbc_url_does_not_include_credentials():
    url = lakebase_jdbc_url("lakebase.example.databricks.com", "5432", "control")

    assert url == "jdbc:postgresql://lakebase.example.databricks.com:5432/control?sslmode=require"
    assert "password" not in url.lower()
    assert "user" not in url.lower()


def test_build_lakebase_control_query_preserves_normalized_contract():
    query = build_lakebase_control_query(
        proceso="Modelo_Priorizacion_Stock",
        sistema_fuente="SapHana",
        propietario_fuente="VistasSapHana",
        anio_mes_dia_inicial="20260101",
        anio_mes_dia_final="20260131",
    )

    assert f"FROM {LAKEBASE_CONTROL_VIEW}" in query
    assert "proceso = 'Modelo_Priorizacion_Stock'" in query
    assert "sistema_fuente = 'SapHana'" in query
    assert "propietario_fuente = 'VistasSapHana'" in query
    assert "extension_archivo_destino" in query
    assert "ORDER BY orden_ejecucion" in query


def test_validate_lakebase_inputs_rejects_invalid_owner():
    try:
        validate_lakebase_inputs("Modelo_Priorizacion_Stock", "SapHana", "Otro")
    except ValueError as exc:
        assert "propietario_fuente invalido" in str(exc)
    else:
        raise AssertionError("validate_lakebase_inputs debio rechazar propietario invalido")


def test_read_lakebase_secret_values_uses_secret_scope():
    dbutils = FakeDbutils()
    values = read_lakebase_secret_values(
        dbutils,
        "scope",
        LakebaseSecretNames(
            host="host",
            port="port",
            database="database",
            username="username",
            password="password",
        ),
    )

    assert values["host"] == "lakebase.example.databricks.com"
    assert values["password"] == "pwd"
    assert ("scope", "password") in dbutils.secrets.calls
