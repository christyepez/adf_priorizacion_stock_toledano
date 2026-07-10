# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Extraccion Bronze - Extraer Sap Hana
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("schema_sap", "sap")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("modo_control", "get_control_cargas")

# Placeholder de extraccion SAP HANA.
# Implementar usando secretos desde secret_scope y control por modo_control.
