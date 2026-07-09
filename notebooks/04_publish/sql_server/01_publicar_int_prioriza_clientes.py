# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Publicacion - Publicar Int Prioriza Clientes
# MAGIC
# MAGIC Publica el resultado Gold hacia SQL Server mediante JDBC y Secret Scope.
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
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("secret_scope", "")

# Placeholder de publicacion a SQL Server.
# Resolver JDBC y credenciales con dbutils.secrets.get(secret_scope, nombre_secreto).
