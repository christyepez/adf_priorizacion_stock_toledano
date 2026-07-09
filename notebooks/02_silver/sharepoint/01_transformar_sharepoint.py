# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("schema_sharepoint", "sharepoint")

# Placeholder de transformaciones SharePoint bronze a silver.
