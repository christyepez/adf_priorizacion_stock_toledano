# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Control De Cargas - Get Control Cargas
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "SapHana")
dbutils.widgets.text("AñoMesDiaInicial", "0")
dbutils.widgets.text("AñoMesDiaFinal", "0")
dbutils.widgets.text("propietario_fuente", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("sql_control_server_secret", "sql-control-server")
dbutils.widgets.text("sql_control_database_secret", "sql-control-database")
dbutils.widgets.text("sql_control_username_secret", "sql-control-username")
dbutils.widgets.text("sql_control_password_secret", "sql-control-password")
dbutils.widgets.text("audit_delta_enabled", "false")
dbutils.widgets.text("audit_delta_table", "")

from priorizacion_stock_toledano.control.get_control_cargas import (
    CONTROL_VIEW_NAME,
    SqlSecretNames,
    build_get_control_cargas_query,
    jdbc_url,
    normalize_spark_dataframe,
    read_get_control_cargas_jdbc,
    read_sql_secret_values,
)

from pyspark.sql.functions import col

ambiente = dbutils.widgets.get("ambiente")
proceso = dbutils.widgets.get("Proceso")
sistema_fuente = dbutils.widgets.get("SistemaFuente")
anio_mes_dia_inicial = dbutils.widgets.get("AñoMesDiaInicial")
anio_mes_dia_final = dbutils.widgets.get("AñoMesDiaFinal")
propietario_fuente = dbutils.widgets.get("propietario_fuente").strip()
secret_scope = dbutils.widgets.get("secret_scope")
audit_delta_enabled = dbutils.widgets.get("audit_delta_enabled").strip().lower() == "true"
audit_delta_table = dbutils.widgets.get("audit_delta_table").strip()

query = build_get_control_cargas_query(
    anio_mes_dia_inicial=anio_mes_dia_inicial,
    anio_mes_dia_final=anio_mes_dia_final,
    proceso=proceso,
    sistema_fuente=sistema_fuente,
)

secret_values = read_sql_secret_values(
    dbutils,
    secret_scope,
    SqlSecretNames(
        server=dbutils.widgets.get("sql_control_server_secret"),
        database=dbutils.widgets.get("sql_control_database_secret"),
        username=dbutils.widgets.get("sql_control_username_secret"),
        password=dbutils.widgets.get("sql_control_password_secret"),
    ),
)

df_raw = read_get_control_cargas_jdbc(
    spark,
    url=jdbc_url(secret_values["server"], secret_values["database"]),
    username=secret_values["username"],
    password=secret_values["password"],
    query=query,
)

df_control = normalize_spark_dataframe(df_raw).filter(col("activo") == True)

if propietario_fuente:
    if propietario_fuente not in {"VistasSapHana", "DatosPortalDeInformacion"}:
        raise ValueError("propietario_fuente debe ser VistasSapHana o DatosPortalDeInformacion")
    df_control = df_control.filter(col("propietario_fuente") == propietario_fuente)

if df_control.limit(1).count() == 0:
    raise ValueError("GetControlCargas no devolvio registros activos para los parametros indicados")

df_control.createOrReplaceTempView(CONTROL_VIEW_NAME)

if audit_delta_enabled:
    if not audit_delta_table:
        raise ValueError("audit_delta_table es obligatorio cuando audit_delta_enabled=true")
    df_control.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_delta_table)
