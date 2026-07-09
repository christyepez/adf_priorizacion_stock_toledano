# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("propietario_fuente", "VistasSapHana")
dbutils.widgets.text("modo_control", "get_control_cargas")

# Placeholder administrado por bundle para filtrar PropietarioFuente SAP.
