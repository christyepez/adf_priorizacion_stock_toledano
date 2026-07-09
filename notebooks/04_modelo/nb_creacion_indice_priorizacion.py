# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Modelo De Priorizacion - Creacion Indice Priorizacion
# MAGIC
# MAGIC Orquesta la parametrizacion y ejecucion del modelo de indice de priorizacion.
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
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("fecha_filtro", "")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_sap", "sap")
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("modo_ejecucion", "normal")

ambiente = dbutils.widgets.get("ambiente").strip()
execution_id = dbutils.widgets.get("execution_id").strip()
fecha_filtro = dbutils.widgets.get("fecha_filtro").strip()
catalog_silver = dbutils.widgets.get("catalog_silver").strip() or None
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or None
schema_sap = dbutils.widgets.get("schema_sap").strip() or "sap"
schema_sharepoint = dbutils.widgets.get("schema_sharepoint").strip() or "sharepoint"
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"

if not ambiente:
    raise ValueError("El parametro 'ambiente' es requerido")

if not execution_id:
    import uuid

    execution_id = str(uuid.uuid4())

from priorizacion_stock_toledano.model.indice_priorizacion import (
    calcular_indice_priorizacion,
    escribir_salidas_gold,
)
from priorizacion_stock_toledano.model.model_parameters import obtener_tablas

tablas_modelo = obtener_tablas(
    ambiente,
    catalog_silver=catalog_silver,
    catalog_gold=catalog_gold,
    schema_sap=schema_sap,
    schema_sharepoint=schema_sharepoint,
    schema_atlas=schema_atlas,
)

df_indice, metrics = calcular_indice_priorizacion(
    spark,
    tablas_modelo,
    ambiente=ambiente,
    execution_id=execution_id,
    fecha_filtro=fecha_filtro or None,
)

print(
    {
        "event": "modelo_creacion_indice_priorizacion_counts",
        "ambiente": ambiente,
        "execution_id": execution_id,
        "input_counts": metrics.input_counts,
        "output_count": metrics.output_count,
        "output_table": metrics.output_table,
        "output_history_table": metrics.output_history_table,
    }
)

escribir_salidas_gold(
    df_indice,
    metrics.output_table,
    metrics.output_history_table,
)

print(
    {
        "event": "modelo_creacion_indice_priorizacion_finished",
        "ambiente": ambiente,
        "execution_id": execution_id,
        "rows_written": metrics.output_count,
    }
)
