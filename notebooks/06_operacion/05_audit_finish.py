# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Auditoria Y Notificacion - Audit Finish
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
# Bootstrap del paquete del proyecto para ejecuciones como Workspace Files o Bundle.
import sys
from pathlib import Path


def _add_project_src_to_path() -> None:
    candidates = []
    cwd = Path.cwd()
    candidates.extend([
        cwd / "src",
        cwd.parent / "src",
        cwd.parent.parent / "src",
        cwd.parent.parent.parent / "src",
    ])
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        workspace_file = Path("/Workspace") / notebook_path.lstrip("/")
        candidates.extend([
            workspace_file.parent / "src",
            workspace_file.parent.parent / "src",
            workspace_file.parent.parent.parent / "src",
            workspace_file.parent.parent.parent.parent / "src",
        ])
    except Exception:
        pass

    for candidate in candidates:
        if (candidate / "priorizacion_stock_toledano").exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return

    raise ModuleNotFoundError(
        "No se encontro src/priorizacion_stock_toledano. "
        "Despliega el bundle completo o instala el paquete wheel en el job cluster."
    )


_add_project_src_to_path()

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "all")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("audit_table", "")
dbutils.widgets.text("status", "SUCCEEDED")
dbutils.widgets.text("message", "Fin exitoso del proceso")

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
status = dbutils.widgets.get("status").strip() or "SUCCEEDED"
message = dbutils.widgets.get("message").strip() or "Fin de ejecucion del pipeline"

audit_table = audit_table_param or audit_table_identifier(catalog_gold, schema_atlas)
event = build_audit_event(
    ambiente=ambiente,
    process=process,
    run_id=run_id,
    status=status,
    name="Priorizacion Stock Toledano",
    message=message,
    task="audit_finish",
    source_system=source_system,
    execution_id=execution_id,
)

append_audit_event(spark, event, audit_table)
print(event.as_dict())
