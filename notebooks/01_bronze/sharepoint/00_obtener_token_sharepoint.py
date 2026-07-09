# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Extraccion Bronze - Obtener Token Sharepoint
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("secret_scope", "")

# Placeholder administrado por bundle para obtener token SharePoint.
# El token debe obtenerse en runtime desde secretos; no persistir ni imprimir.
