# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Auditoria Y Notificacion - Notify
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

from priorizacion_stock_toledano.config import define_text_widget

define_text_widget(dbutils, "ambiente", "dev")
define_text_widget(dbutils, "Proceso", "Modelo_Priorizacion_Stock")
define_text_widget(dbutils, "run_id", "")
define_text_widget(dbutils, "execution_id", "")
define_text_widget(dbutils, "status", "SUCCEEDED")
define_text_widget(dbutils, "emails", "")
define_text_widget(dbutils, "message", "Fin exitoso del proceso")
define_text_widget(dbutils, "subject", "Priorizacion Stock Toledano")
define_text_widget(dbutils, "name", "Priorizacion Stock Toledano")
define_text_widget(dbutils, "secret_scope", "")
define_text_widget(dbutils, "notification_endpoint_secret", "")
define_text_widget(dbutils, "notification_enabled", "false")
define_text_widget(dbutils, "catalog_gold", "")
define_text_widget(dbutils, "schema_atlas", "atlas")
define_text_widget(dbutils, "audit_table", "")
define_text_widget(dbutils, "original_error", "")

from priorizacion_stock_toledano.audit.audit_logger import (
    append_audit_event,
    audit_table_identifier,
    build_audit_event,
)
from priorizacion_stock_toledano.audit.notifier import (
    build_notification_payload,
    read_notification_endpoint,
    send_notification,
)

ambiente = dbutils.widgets.get("ambiente").strip()
process = dbutils.widgets.get("Proceso").strip() or "Modelo_Priorizacion_Stock"
run_id = dbutils.widgets.get("run_id").strip()
execution_id = dbutils.widgets.get("execution_id").strip() or run_id
status = dbutils.widgets.get("status").strip() or "SUCCEEDED"
emails = dbutils.widgets.get("emails").strip()
message = dbutils.widgets.get("message").strip()
subject = dbutils.widgets.get("subject").strip() or "Priorizacion Stock Toledano"
name = dbutils.widgets.get("name").strip() or "Priorizacion Stock Toledano"
secret_scope = dbutils.widgets.get("secret_scope").strip()
notification_endpoint_secret = dbutils.widgets.get("notification_endpoint_secret").strip()
notification_enabled = dbutils.widgets.get("notification_enabled").strip().lower() == "true"
catalog_gold = dbutils.widgets.get("catalog_gold").strip()
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"
audit_table_param = dbutils.widgets.get("audit_table").strip()
original_error = dbutils.widgets.get("original_error").strip()

audit_table = audit_table_param or audit_table_identifier(catalog_gold, schema_atlas)
payload = build_notification_payload(
    emails=emails,
    message=message,
    name=name,
    process=process,
    run_id=run_id,
    status=status,
    subject=subject,
)

try:
    if notification_enabled:
        endpoint = read_notification_endpoint(dbutils, secret_scope, notification_endpoint_secret)
        result = send_notification(endpoint, payload)
        if result.status != "success":
            raise RuntimeError(result.error_message or f"HTTP status {result.status_code}")
        notification_result = result.as_dict()
        event_message = f"Notificacion enviada: {subject}"
    else:
        notification_result = {"status": "disabled"}
        event_message = "Notificacion omitida: notification_enabled=false"
    event = build_audit_event(
        ambiente=ambiente,
        process=process,
        run_id=run_id,
        status=status,
        name=name,
        message=event_message,
        task="notify",
        execution_id=execution_id,
    )
    append_audit_event(spark, event, audit_table)
    print({"payload": payload, "notification": notification_result})
except Exception as notify_error:
    warning_event = build_audit_event(
        ambiente=ambiente,
        process=process,
        run_id=run_id,
        status="WARNING",
        name=name,
        message=f"Fallo envio de notificacion: {notify_error}",
        task="notify",
        execution_id=execution_id,
    )
    append_audit_event(spark, warning_event, audit_table)
    print({"payload": payload, "notification_warning": str(notify_error)})
    if status.strip().upper() == "FAILED" and original_error:
        raise RuntimeError(original_error)
