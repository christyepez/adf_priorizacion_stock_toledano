# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_sap", "sap")
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("schema_atlas", "atlas")

# Placeholder de calculo gold del modelo Priorizacion Stock.
