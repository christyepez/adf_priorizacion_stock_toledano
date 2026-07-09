# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Extraccion Bronze - Extraer Sharepoint
# MAGIC
# MAGIC Extrae archivos SharePoint definidos por control de cargas usando secretos seguros y escribe la capa Bronze.
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
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("modo_control", "get_control_cargas")

# Placeholder de extraccion SharePoint.
# Implementar OAuth/Graph/SharePoint usando secretos desde secret_scope.
