# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Auditoria Y Notificacion - Audit Start
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
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("audit_table", "")

from priorizacion_stock_toledano.audit.audit_logger import (
    append_audit_event,
    audit_table_identifier,
    build_audit_event,
)

ambiente = dbutils.widgets.get("ambiente").strip()
process = dbutils.widgets.get("Proceso").strip() or "Modelo_Priorizacion_Stock"
source_system = dbutils.widgets.get("SistemaFuente").strip() or "all"
run_id = dbutils.widgets.get("run_id").strip()
execution_id = dbutils.widgets.get("execution_id").strip() or run_id
catalog_gold = dbutils.widgets.get("catalog_gold").strip()
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"
audit_table_param = dbutils.widgets.get("audit_table").strip()

audit_table = audit_table_param or audit_table_identifier(catalog_gold, schema_atlas)
event = build_audit_event(
    ambiente=ambiente,
    process=process,
    run_id=run_id,
    status="STARTED",
    name="Priorizacion Stock Toledano",
    message="Inicio de ejecucion del pipeline",
    task="audit_start",
    source_system=source_system,
    execution_id=execution_id,
)

append_audit_event(spark, event, audit_table)
print(event.as_dict())
