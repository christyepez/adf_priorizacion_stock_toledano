# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Auditoria Y Notificacion - Notify Success
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("emails", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("notification_endpoint_secret", "")

# Placeholder administrado por bundle para notificacion exitosa.
# El endpoint debe resolverse desde secret_scope; no incluir URLs firmadas.
