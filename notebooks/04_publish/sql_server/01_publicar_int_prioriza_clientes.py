# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Publicacion - Publicar Int Prioriza Clientes
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("secret_scope", "")

# Placeholder de publicacion a SQL Server.
# Resolver JDBC y credenciales con dbutils.secrets.get(secret_scope, nombre_secreto).
