# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_sap", "sap")
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("schema_atlas", "atlas")

from priorizacion_stock_toledano.model.model_parameters import obtener_tablas

ambiente = dbutils.widgets.get("ambiente")
catalog_silver = dbutils.widgets.get("catalog_silver").strip() or None
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or None
schema_sap = dbutils.widgets.get("schema_sap").strip() or "sap"
schema_sharepoint = dbutils.widgets.get("schema_sharepoint").strip() or "sharepoint"
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"

tablas_modelo = obtener_tablas(
    ambiente,
    catalog_silver=catalog_silver,
    catalog_gold=catalog_gold,
    schema_sap=schema_sap,
    schema_sharepoint=schema_sharepoint,
    schema_atlas=schema_atlas,
)
