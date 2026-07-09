# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Calidad Y Auditoria - Audit Start
# MAGIC
# MAGIC Registra eventos operativos y gestiona notificaciones sin exponer endpoints ni secretos.
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
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "all")
dbutils.widgets.text("modo_control", "get_control_cargas")
dbutils.widgets.text("modo_ejecucion", "normal")

# Placeholder administrado por bundle para registrar inicio de ejecucion.
