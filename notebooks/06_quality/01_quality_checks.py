# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Calidad Y Auditoria - Quality Checks
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")

# Placeholder administrado por bundle para validaciones de calidad.
