# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Transformacion Bronze To Silver - Transformar Sharepoint
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("schema_sharepoint", "sharepoint")

# Placeholder de transformaciones SharePoint bronze a silver.
