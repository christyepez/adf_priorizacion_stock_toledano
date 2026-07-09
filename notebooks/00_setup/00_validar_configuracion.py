# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Setup - Validar Configuracion
# MAGIC
# MAGIC Valida configuracion minima requerida antes de ejecutar el pipeline.
# MAGIC
# MAGIC **Proyecto:** Priorizacion de Stock Toledano.
# MAGIC **Migracion:** Azure Data Factory a Databricks Asset Bundles.

# COMMAND ----------
# Comentarios de mantenimiento:
# - Mantener este notebook como orquestador de la etapa correspondiente.
# - Ubicar la logica reutilizable en src/priorizacion_stock_toledano.
# - Resolver credenciales, endpoints y tokens exclusivamente desde Secret Scope.
# - No imprimir secretos ni URLs firmadas en logs o salidas del notebook.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parametros y configuracion de entrada
# MAGIC Los widgets definidos a continuacion son inyectados por Databricks Jobs o por ejecuciones manuales controladas.

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
