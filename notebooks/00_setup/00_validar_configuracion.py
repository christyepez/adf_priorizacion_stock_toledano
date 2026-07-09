# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("modo_control", "get_control_cargas")
dbutils.widgets.text("secret_scope", "")

ambiente = dbutils.widgets.get("ambiente")
modo_control = dbutils.widgets.get("modo_control")
secret_scope = dbutils.widgets.get("secret_scope")

if modo_control not in {"get_control_cargas", "lakebase"}:
    raise ValueError("modo_control debe ser get_control_cargas o lakebase")

if not ambiente.strip():
    raise ValueError("ambiente es obligatorio")

if not secret_scope.strip():
    raise ValueError("secret_scope es obligatorio")
