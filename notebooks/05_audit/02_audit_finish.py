# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Calidad Y Auditoria - Audit Finish
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "all")
dbutils.widgets.text("modo_control", "get_control_cargas")
dbutils.widgets.text("modo_ejecucion", "normal")

# Placeholder administrado por bundle para registrar fin de ejecucion.
