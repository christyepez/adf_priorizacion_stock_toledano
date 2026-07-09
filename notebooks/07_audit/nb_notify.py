# Databricks notebook source
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("status", "SUCCEEDED")
dbutils.widgets.text("emails", "")
dbutils.widgets.text("message", "Fin exitoso del proceso")
dbutils.widgets.text("subject", "Priorizacion Stock Toledano")
dbutils.widgets.text("name", "Priorizacion Stock Toledano")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("notification_endpoint_secret", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("audit_table", "")
dbutils.widgets.text("original_error", "")

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
    endpoint = read_notification_endpoint(dbutils, secret_scope, notification_endpoint_secret)
    result = send_notification(endpoint, payload)
    if result.status != "success":
        raise RuntimeError(result.error_message or f"HTTP status {result.status_code}")
    event = build_audit_event(
        ambiente=ambiente,
        process=process,
        run_id=run_id,
        status=status,
        name=name,
        message=f"Notificacion enviada: {subject}",
        task="notify",
        execution_id=execution_id,
    )
    append_audit_event(spark, event, audit_table)
    print({"payload": payload, "notification": result.as_dict()})
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
