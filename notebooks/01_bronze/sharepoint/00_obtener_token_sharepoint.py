# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("secret_scope", "")

# Placeholder administrado por bundle para obtener token SharePoint.
# El token debe obtenerse en runtime desde secretos; no persistir ni imprimir.
