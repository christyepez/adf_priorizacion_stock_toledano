# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Extraccion Bronze - Ext Saphana Priorizacion Stock
# MAGIC
# MAGIC Extrae objetos SAP HANA definidos por control de cargas y registra metricas tecnicas por objeto.
# MAGIC
# MAGIC **Proyecto:** Priorizacion de Stock Toledano.
# MAGIC **Migracion:** Azure Data Factory a Databricks Asset Bundles.

# COMMAND ----------
# Comentarios de mantenimiento:
# - Mantener este notebook como orquestador de la etapa correspondiente.
# - Ubicar la logica reutilizable en src/priorizacion_stock_toledano.
# - Resolver credenciales, endpoints y tokens exclusivamente desde Secret Scope.
# - No imprimir secretos ni URLs firmadas en logs o salidas del notebook.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parametros y configuracion de entrada
# MAGIC Los widgets definidos a continuacion son inyectados por Databricks Jobs o por ejecuciones manuales controladas.

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "SapHana")
dbutils.widgets.text("AñoMesDiaInicial", "0")
dbutils.widgets.text("AñoMesDiaFinal", "0")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("fail_fast", "true")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("sql_control_server_secret", "sql-control-server")
dbutils.widgets.text("sql_control_database_secret", "sql-control-database")
dbutils.widgets.text("sql_control_username_secret", "sql-control-username")
dbutils.widgets.text("sql_control_password_secret", "sql-control-password")
dbutils.widgets.text("sap_hana_server_secret", "sap-hana-server")
dbutils.widgets.text("sap_hana_port_secret", "")
dbutils.widgets.text("sap_hana_username_secret", "sap-hana-username")
dbutils.widgets.text("sap_hana_password_secret", "sap-hana-password")
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
secret_scope = dbutils.widgets.get("secret_scope")
storage_account = dbutils.widgets.get("storage_account")
metrics_delta_table = dbutils.widgets.get("metrics_delta_table").strip()

control_secrets = read_sql_secret_values(
    dbutils,
    secret_scope,
    SqlSecretNames(
        server=dbutils.widgets.get("sql_control_server_secret"),
        database=dbutils.widgets.get("sql_control_database_secret"),
        username=dbutils.widgets.get("sql_control_username_secret"),
        password=dbutils.widgets.get("sql_control_password_secret"),
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
    url=jdbc_url(control_secrets["server"], control_secrets["database"]),
    username=control_secrets["username"],
    password=control_secrets["password"],
    query=control_query,
)

df_control = (
    normalize_spark_dataframe(df_control_raw)
    .filter(col("activo") == True)
    .filter(col("propietario_fuente") == "VistasSapHana")
    .orderBy(col("orden_ejecucion").asc(), col("nombre_archivo_fuente").asc())
)

control_rows = [row.asDict(recursive=True) for row in df_control.collect()]
if not control_rows:
    raise ValueError("No existen registros activos de control para PropietarioFuente=VistasSapHana")

sap_port_secret = dbutils.widgets.get("sap_hana_port_secret").strip() or None
sap_secrets = read_sap_secret_values(
    dbutils,
    secret_scope,
    SapHanaSecretNames(
        server=dbutils.widgets.get("sap_hana_server_secret"),
        port=sap_port_secret,
        username=dbutils.widgets.get("sap_hana_username_secret"),
        password=dbutils.widgets.get("sap_hana_password_secret"),
    ),
)
sap_url = hana_jdbc_url(sap_secrets["server"], sap_secrets.get("port"))

metrics = []
errors = []

for record in control_rows:
    source_object = f"{record['ruta_archivo_fuente']}.{record['nombre_archivo_fuente']}"
    target_path = build_bronze_path(
        storage_account=storage_account,
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
