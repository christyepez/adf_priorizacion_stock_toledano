# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Transformacion Bronze To Silver - Transformar Sap
# MAGIC
# MAGIC Aplica limpieza, tipado y escritura Delta para transformar datos Bronze hacia Silver.
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
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("schema_sap", "sap")

# Placeholder de transformaciones SAP bronze a silver.
