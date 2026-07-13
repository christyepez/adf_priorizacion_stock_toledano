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

from priorizacion_stock_toledano.config import define_text_widget

define_text_widget(dbutils, "ambiente", "dev")
define_text_widget(dbutils, "Proceso", "Modelo_Priorizacion_Stock")
define_text_widget(dbutils, "SistemaFuente", "SapHana")
define_text_widget(dbutils, "AñoMesDiaInicial", "0")
define_text_widget(dbutils, "AñoMesDiaFinal", "0")
define_text_widget(dbutils, "execution_id", "")
define_text_widget(dbutils, "fail_fast", "true")
define_text_widget(dbutils, "secret_scope", "")
define_text_widget(dbutils, "storage_account_name", "")
define_text_widget(dbutils, "sql_control_server", "")
define_text_widget(dbutils, "sql_control_database", "")
define_text_widget(dbutils, "sql_control_schema", "conf")
define_text_widget(dbutils, "sql_control_table", "ControlCargas")
define_text_widget(dbutils, "sql_control_read_mode", "jdbc_table")
define_text_widget(dbutils, "sql_control_spark_sql", "")
define_text_widget(dbutils, "sql_control_spark_relation", "")
define_text_widget(dbutils, "sql_control_server_secret", "")
define_text_widget(dbutils, "sql_control_database_secret", "")
define_text_widget(dbutils, "sql_control_username_secret", "")
define_text_widget(dbutils, "sql_control_password_secret", "")
define_text_widget(dbutils, "sql_control_encrypt", "true")
define_text_widget(dbutils, "sql_control_trust_server_certificate", "false")
define_text_widget(dbutils, "sap_hana_server_secret", "")
define_text_widget(dbutils, "sap_hana_port_secret", "")
define_text_widget(dbutils, "sap_hana_username_secret", "")
define_text_widget(dbutils, "sap_hana_password_secret", "")
define_text_widget(dbutils, "sap_hana_driver", "com.sap.db.jdbc.Driver")
define_text_widget(dbutils, "sap_hana_jdbc_jar", "dbfs:/FileStore/jars/sap/ngdbc-2.28.7.jar")
define_text_widget(dbutils, "sap_hana_propietario_fuente", "VistasSapHana")
define_text_widget(dbutils, "metrics_delta_table", "")

from uuid import uuid4

from priorizacion_stock_toledano.control.get_control_cargas import (
    SqlSecretNames,
    build_get_control_cargas_query,
    build_get_control_cargas_table_query,
    jdbc_url,
    normalize_spark_dataframe,
    read_get_control_cargas_spark_sql,
    read_get_control_cargas_jdbc,
    read_sql_secret_values,
    resolve_get_control_cargas_spark_sql,
    validate_control_read_mode,
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
    register_saphana_jdbc_jar,
    sap_metrics_schema,
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
secret_scope = dbutils.widgets.get("secret_scope").strip()
storage_account_name = dbutils.widgets.get("storage_account_name").strip()
sql_control_server = dbutils.widgets.get("sql_control_server").strip()
sql_control_database = dbutils.widgets.get("sql_control_database").strip()
sql_control_schema = dbutils.widgets.get("sql_control_schema").strip()
sql_control_table = dbutils.widgets.get("sql_control_table").strip()
sql_control_read_mode = validate_control_read_mode(dbutils.widgets.get("sql_control_read_mode"))
sql_control_spark_sql = dbutils.widgets.get("sql_control_spark_sql").strip()
sql_control_spark_relation = dbutils.widgets.get("sql_control_spark_relation").strip()
sql_control_encrypt = dbutils.widgets.get("sql_control_encrypt").strip() or "true"
sql_control_trust_server_certificate = (
    dbutils.widgets.get("sql_control_trust_server_certificate").strip() or "false"
)
sap_hana_propietario_fuente = dbutils.widgets.get("sap_hana_propietario_fuente").strip() or "VistasSapHana"
sap_hana_driver = dbutils.widgets.get("sap_hana_driver").strip() or "com.sap.db.jdbc.Driver"
sap_hana_jdbc_jar = dbutils.widgets.get("sap_hana_jdbc_jar").strip()
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

if sql_control_read_mode == "spark_sql":
    control_query = resolve_get_control_cargas_spark_sql(
        spark_sql=sql_control_spark_sql,
        relation=sql_control_spark_relation,
        anio_mes_dia_inicial=anio_mes_dia_inicial,
        anio_mes_dia_final=anio_mes_dia_final,
        proceso=process_name,
        sistema_fuente=sistema_fuente,
    )
    df_control_raw = read_get_control_cargas_spark_sql(spark, control_query)
else:
    if sql_control_read_mode == "jdbc_table":
        control_query = build_get_control_cargas_table_query(
            schema=sql_control_schema,
            table=sql_control_table,
            anio_mes_dia_inicial=anio_mes_dia_inicial,
            anio_mes_dia_final=anio_mes_dia_final,
            proceso=process_name,
            sistema_fuente=sistema_fuente,
        )
    else:
        control_query = build_get_control_cargas_query(
            anio_mes_dia_inicial=anio_mes_dia_inicial,
            anio_mes_dia_final=anio_mes_dia_final,
            proceso=process_name,
            sistema_fuente=sistema_fuente,
        )
    control_secrets = read_sql_secret_values(
        dbutils,
        secret_scope,
        SqlSecretNames(
            server=dbutils.widgets.get("sql_control_server_secret").strip(),
            database=dbutils.widgets.get("sql_control_database_secret").strip(),
            username=dbutils.widgets.get("sql_control_username_secret").strip(),
            password=dbutils.widgets.get("sql_control_password_secret").strip(),
            server_value=sql_control_server,
            database_value=sql_control_database,
        ),
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
        server=dbutils.widgets.get("sap_hana_server_secret").strip(),
        port=sap_port_secret,
        username=dbutils.widgets.get("sap_hana_username_secret").strip(),
        password=dbutils.widgets.get("sap_hana_password_secret").strip(),
    ),
)
sap_url = hana_jdbc_url(sap_secrets["server"], sap_secrets.get("port"))
register_saphana_jdbc_jar(spark, sap_hana_jdbc_jar)

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

df_metrics = spark.createDataFrame(metrics, schema=sap_metrics_schema())
df_metrics.createOrReplaceTempView(SAP_METRICS_VIEW_NAME)

if metrics_delta_table:
    df_metrics.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(metrics_delta_table)

if errors:
    formatted = "; ".join([f"{source}: {message}" for source, message in errors])
    raise RuntimeError(f"Errores durante extraccion SAP HANA: {formatted}")
