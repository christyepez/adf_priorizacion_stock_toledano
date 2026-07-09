from priorizacion_stock_toledano.publication.sql_publisher import (
    REQUIRED_PUBLICATION_COLUMNS,
    SqlPublicationSecretNames,
    jdbc_url,
    publish_results_to_sql,
    read_sql_publication_secret_values,
    sanitize_error_message,
    target_table_identifier,
    validate_publication_columns,
    validate_publication_mode,
    write_publication_jdbc,
)


class FakeSecrets:
    def __init__(self):
        self.calls = []
        self.values = {
            ("scope", "server-secret"): "sql.example.database.windows.net",
            ("scope", "database-secret"): "db",
            ("scope", "username-secret"): "user",
            ("scope", "password-secret"): "pwd",
        }

    def get(self, scope, key):
        self.calls.append((scope, key))
        return self.values[(scope, key)]


class FakeDbutils:
    def __init__(self):
        self.secrets = FakeSecrets()


class FakeWriter:
    def __init__(self):
        self.format_value = None
        self.mode_value = None
        self.options = {}
        self.saved = False

    def format(self, value):
        self.format_value = value
        return self

    def mode(self, value):
        self.mode_value = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def save(self):
        self.saved = True


class FakeDataFrame:
    def __init__(self, columns=None, row_count=3):
        self.columns = columns or list(REQUIRED_PUBLICATION_COLUMNS)
        self.row_count = row_count
        self.selected = None
        self.write = FakeWriter()

    def select(self, *columns):
        selected = FakeDataFrame(list(columns), self.row_count)
        selected.selected = list(columns)
        return selected

    def count(self):
        return self.row_count


class FakeSpark:
    def __init__(self, df):
        self.df = df
        self.source_table = None

    def table(self, name):
        self.source_table = name
        return self.df


def test_jdbc_url_has_no_credentials():
    url = jdbc_url("sql.example.database.windows.net", "pub")

    assert "jdbc:sqlserver://sql.example.database.windows.net;" in url
    assert "databaseName=pub" in url
    assert "password" not in url.lower()
    assert "user=" not in url.lower()


def test_target_table_identifier_defaults_to_adf_target():
    assert target_table_identifier() == "dbo.Int_Prioriza_Clientes"


def test_validate_publication_mode_accepts_required_modes():
    assert validate_publication_mode("append") == "append"
    assert validate_publication_mode("overwrite") == "overwrite"
    assert validate_publication_mode("truncate_insert") == "truncate_insert"


def test_validate_publication_columns_rejects_missing_column():
    df = FakeDataFrame(columns=["cod_cliente", "prioridad", "Estatus"])

    try:
        validate_publication_columns(df)
    except ValueError as exc:
        assert "create_timestamp" in str(exc)
    else:
        raise AssertionError("validate_publication_columns debio rechazar columnas faltantes")


def test_read_sql_publication_secret_values_uses_secret_scope():
    dbutils = FakeDbutils()
    values = read_sql_publication_secret_values(
        dbutils,
        "scope",
        SqlPublicationSecretNames(
            server="server-secret",
            database="database-secret",
            username="username-secret",
            password="password-secret",
        ),
    )

    assert values["server"] == "sql.example.database.windows.net"
    assert values["password"] == "pwd"
    assert ("scope", "password-secret") in dbutils.secrets.calls


def test_write_publication_jdbc_truncate_insert_uses_overwrite_and_truncate():
    df = FakeDataFrame()
    write_publication_jdbc(
        df,
        url="jdbc:sqlserver://server;databaseName=db;",
        username="user",
        password="pwd",
        target_table="dbo.Int_Prioriza_Clientes",
        mode="truncate_insert",
    )

    assert df.write.format_value == "jdbc"
    assert df.write.mode_value == "overwrite"
    assert df.write.options["truncate"] == "true"
    assert df.write.options["dbtable"] == "dbo.Int_Prioriza_Clientes"
    assert df.write.saved is True


def test_publish_results_to_sql_returns_success_metrics():
    spark = FakeSpark(FakeDataFrame(row_count=7))
    metrics = publish_results_to_sql(
        spark,
        source_table="toledano_gold_dev.atlas.resultados_indice_priorizacion",
        url="jdbc:sqlserver://server;databaseName=db;",
        username="user",
        password="pwd",
        mode="append",
    )

    assert spark.source_table == "toledano_gold_dev.atlas.resultados_indice_priorizacion"
    assert metrics.rows_read == 7
    assert metrics.rows_written == 7
    assert metrics.target_table == "dbo.Int_Prioriza_Clientes"
    assert metrics.status == "success"


def test_publish_results_to_sql_returns_error_metrics_without_secret_leak():
    spark = FakeSpark(FakeDataFrame(columns=["cod_cliente"]))
    metrics = publish_results_to_sql(
        spark,
        source_table="gold.table",
        url="jdbc:sqlserver://server;databaseName=db;",
        username="user",
        password="pwd",
        mode="append",
    )

    assert metrics.status == "error"
    assert metrics.rows_written == 0
    assert "prioridad" in metrics.error_message


def test_sanitize_error_message_redacts_credentials():
    message = "Login failed for user user with password pwd"

    sanitized = sanitize_error_message(message, "user", "pwd")

    assert "user" not in sanitized
    assert "pwd" not in sanitized
    assert "<redacted>" in sanitized
