# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Extraccion Bronze - Ext Saphana Priorizacion Stock
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
# Bootstrap del paquete del proyecto para ejecuciones como Workspace Files o Bundle.
import sys
from pathlib import Path


def _add_project_src_to_path() -> None:
    candidates = []
    cwd = Path.cwd()
    candidates.extend([
        cwd / "src",
        cwd.parent / "src",
        cwd.parent.parent / "src",
        cwd.parent.parent.parent / "src",
    ])
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        workspace_file = Path("/Workspace") / notebook_path.lstrip("/")
        candidates.extend([
            workspace_file.parent / "src",
            workspace_file.parent.parent / "src",
            workspace_file.parent.parent.parent / "src",
            workspace_file.parent.parent.parent.parent / "src",
        ])
    except Exception:
        pass

    for candidate in candidates:
        if (candidate / "priorizacion_stock_toledano").exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return

    raise ModuleNotFoundError(
        "No se encontro src/priorizacion_stock_toledano. "
        "Despliega el bundle completo o instala el paquete wheel en el job cluster."
    )


_add_project_src_to_path()

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "SapHana")
dbutils.widgets.text("AñoMesDiaInicial", "0")
dbutils.widgets.text("AñoMesDiaFinal", "0")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("fail_fast", "true")
dbutils.widgets.text("secret_scope", "kv-bigd-toledano-dev-01")
dbutils.widgets.text("storage_account_name", "dlsbigdatatoledanodev")
dbutils.widgets.text("sql_control_server", "")
dbutils.widgets.text("sql_control_database", "")
dbutils.widgets.text("sql_control_server_secret", "sc-sqlbigdatatoledano-server")
dbutils.widgets.text("sql_control_database_secret", "sc-sqlbigdatatoledano-database")
dbutils.widgets.text("sql_control_username_secret", "sc-sqlbigdatatoledano-username")
dbutils.widgets.text("sql_control_password_secret", "sc-sqlbigdatatoledano-password")
dbutils.widgets.text("sql_control_encrypt", "true")
dbutils.widgets.text("sql_control_trust_server_certificate", "false")
dbutils.widgets.text("sap_hana_server_secret", "sc-saphana-servernode")
dbutils.widgets.text("sap_hana_port_secret", "")
dbutils.widgets.text("sap_hana_username_secret", "sc-saphana-username")
dbutils.widgets.text("sap_hana_password_secret", "sc-saphana-password")
dbutils.widgets.text("sap_hana_driver", "com.sap.db.jdbc.Driver")
dbutils.widgets.text("sap_hana_propietario_fuente", "VistasSapHana")
dbutils.widgets.text("metrics_delta_table", "")

from uuid import uuid4

from priorizacion_stock_toledano.control.get_control_cargas import (
    SqlSecretNames,
    build_get_control_cargas_query,
    jdbc_url,
    normalize_spark_dataframe,
    read_get_control_cargas_jdbc,
    read_sql_secret_values,
)
from priorizacion_stock_toledano.extraction.saphana_extractor import (
    SAP_METRICS_VIEW_NAME,
    SAP_SOURCE_SYSTEM,
    SapHanaSecretNames,
    add_technical_columns,
    build_bronze_path,
    build_saphana_query,
    choose_output_format,
    hana_jdbc_url,
    metric_record,
    read_sap_secret_values,
    read_saphana_jdbc,
    write_bronze,
)

from pyspark.sql.functions import col

ambiente = dbutils.widgets.get("ambiente")
process_name = dbutils.widgets.get("Proceso")
sistema_fuente = dbutils.widgets.get("SistemaFuente")
anio_mes_dia_inicial = dbutils.widgets.get("AñoMesDiaInicial")
anio_mes_dia_final = dbutils.widgets.get("AñoMesDiaFinal")
execution_id = dbutils.widgets.get("execution_id").strip() or str(uuid4())
fail_fast = dbutils.widgets.get("fail_fast").strip().lower() == "true"
secret_scope = dbutils.widgets.get("secret_scope").strip() or "kv-bigd-toledano-dev-01"
storage_account_name = dbutils.widgets.get("storage_account_name").strip() or "dlsbigdatatoledanodev"
sql_control_server = dbutils.widgets.get("sql_control_server").strip()
sql_control_database = dbutils.widgets.get("sql_control_database").strip()
sql_control_encrypt = dbutils.widgets.get("sql_control_encrypt").strip() or "true"
sql_control_trust_server_certificate = (
    dbutils.widgets.get("sql_control_trust_server_certificate").strip() or "false"
)
sap_hana_propietario_fuente = dbutils.widgets.get("sap_hana_propietario_fuente").strip() or "VistasSapHana"
sap_hana_driver = dbutils.widgets.get("sap_hana_driver").strip() or "com.sap.db.jdbc.Driver"
metrics_delta_table = dbutils.widgets.get("metrics_delta_table").strip()

if not secret_scope.strip():
    raise ValueError(
        "secret_scope es obligatorio. Ejecuta el notebook desde el Databricks Job/Bundle "
        "o configura el widget secret_scope con el scope respaldado por Key Vault."
    )

if not storage_account_name.strip():
    raise ValueError(
        "storage_account_name es obligatorio. Ejecuta el notebook desde el Databricks Job/Bundle "
        "o configura el widget storage_account_name."
    )

control_secrets = read_sql_secret_values(
    dbutils,
    secret_scope,
    SqlSecretNames(
        server=dbutils.widgets.get("sql_control_server_secret").strip() or "sc-sqlbigdatatoledano-server",
        database=dbutils.widgets.get("sql_control_database_secret").strip() or "sc-sqlbigdatatoledano-database",
        username=dbutils.widgets.get("sql_control_username_secret").strip() or "sc-sqlbigdatatoledano-username",
        password=dbutils.widgets.get("sql_control_password_secret").strip() or "sc-sqlbigdatatoledano-password",
        server_value=sql_control_server,
        database_value=sql_control_database,
    ),
)

control_query = build_get_control_cargas_query(
    anio_mes_dia_inicial=anio_mes_dia_inicial,
    anio_mes_dia_final=anio_mes_dia_final,
    proceso=process_name,
    sistema_fuente=sistema_fuente,
)

df_control_raw = read_get_control_cargas_jdbc(
    spark,
    url=jdbc_url(
        control_secrets["server"],
        control_secrets["database"],
        encrypt=sql_control_encrypt,
        trust_server_certificate=sql_control_trust_server_certificate,
    ),
    username=control_secrets["username"],
    password=control_secrets["password"],
    query=control_query,
)

df_control = (
    normalize_spark_dataframe(df_control_raw)
    .filter(col("activo") == True)
    .filter(col("propietario_fuente") == sap_hana_propietario_fuente)
    .orderBy(col("orden_ejecucion").asc(), col("nombre_archivo_fuente").asc())
)

control_rows = [row.asDict(recursive=True) for row in df_control.collect()]
if not control_rows:
    raise ValueError(f"No existen registros activos de control para PropietarioFuente={sap_hana_propietario_fuente}")

sap_port_secret = dbutils.widgets.get("sap_hana_port_secret").strip() or None
sap_secrets = read_sap_secret_values(
    dbutils,
    secret_scope,
    SapHanaSecretNames(
        server=dbutils.widgets.get("sap_hana_server_secret").strip() or "sc-saphana-servernode",
        port=sap_port_secret,
        username=dbutils.widgets.get("sap_hana_username_secret").strip() or "sc-saphana-username",
        password=dbutils.widgets.get("sap_hana_password_secret").strip() or "sc-saphana-password",
    ),
)
sap_url = hana_jdbc_url(sap_secrets["server"], sap_secrets.get("port"))

metrics = []
errors = []

for record in control_rows:
    source_object = f"{record['ruta_archivo_fuente']}.{record['nombre_archivo_fuente']}"
    target_path = build_bronze_path(
        storage_account=storage_account_name,
        folder=record["ruta_archivo_destino"],
        filename=record["nombre_archivo_destino"],
        extension=record["extension_archivo_destino"],
    )
    try:
        query = build_saphana_query(record)
        df_source = read_saphana_jdbc(
            spark,
            url=sap_url,
            username=sap_secrets["username"],
            password=sap_secrets["password"],
            query=query,
            driver=sap_hana_driver,
        )
        rows_read = df_source.count()
        df_bronze = add_technical_columns(
            df_source,
            source_system=SAP_SOURCE_SYSTEM,
            process_name=process_name,
            execution_id=execution_id,
            source_object=source_object,
        )
        rows_written = write_bronze(
            df_bronze,
            path=target_path,
            output_format=choose_output_format(record),
        )
        metrics.append(
            metric_record(
                process_name=process_name,
                sistema_fuente=sistema_fuente,
                source_object=source_object,
                target_path=target_path,
                rows_read=rows_read,
                rows_written=rows_written,
                status="success",
            )
        )
    except Exception as exc:
        error_message = str(exc)
        errors.append((source_object, error_message))
        metrics.append(
            metric_record(
                process_name=process_name,
                sistema_fuente=sistema_fuente,
                source_object=source_object,
                target_path=target_path,
                rows_read=0,
                rows_written=0,
                status="failed",
                error_message=error_message,
            )
        )
        if fail_fast:
            break

df_metrics = spark.createDataFrame(metrics)
df_metrics.createOrReplaceTempView(SAP_METRICS_VIEW_NAME)

if metrics_delta_table:
    df_metrics.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(metrics_delta_table)

if errors:
    formatted = "; ".join([f"{source}: {message}" for source, message in errors])
    raise RuntimeError(f"Errores durante extraccion SAP HANA: {formatted}")
