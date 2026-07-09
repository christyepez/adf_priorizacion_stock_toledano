# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("modo_control", "get_control_cargas")

# Placeholder de extraccion SharePoint.
# Implementar OAuth/Graph/SharePoint usando secretos desde secret_scope.
