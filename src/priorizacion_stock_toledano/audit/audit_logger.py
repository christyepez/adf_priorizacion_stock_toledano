from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


AUDIT_STATUSES = {"STARTED", "SUCCEEDED", "FAILED", "WARNING"}
DEFAULT_AUDIT_TABLE = "audit_pipeline_events"


@dataclass(frozen=True)
class AuditEvent:
    event_id: str
    timestamp: str
    ambiente: str
    process: str
    run_id: str
    status: str
    name: str
    message: str
    task: str | None = None
    source_system: str | None = None
    execution_id: str | None = None
    details_json: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def validate_audit_status(status: str) -> str:
    clean_status = (status or "").strip().upper()
    if clean_status not in AUDIT_STATUSES:
        allowed = ", ".join(sorted(AUDIT_STATUSES))
        raise ValueError(f"Estado de auditoria invalido: {status!r}. Valores permitidos: {allowed}")
    return clean_status


def audit_table_identifier(catalog_gold: str, schema_atlas: str, table_name: str = DEFAULT_AUDIT_TABLE) -> str:
    catalog = (catalog_gold or "").strip()
    schema = (schema_atlas or "").strip()
    table = (table_name or "").strip()
    if not catalog or not schema or not table:
        raise ValueError("catalog_gold, schema_atlas y table_name son obligatorios para auditoria")
    return f"{catalog}.{schema}.{table}"


def build_audit_event(
    *,
    ambiente: str,
    process: str,
    run_id: str,
    status: str,
    name: str,
    message: str,
    task: str | None = None,
    source_system: str | None = None,
    execution_id: str | None = None,
    details_json: str | None = None,
    timestamp: str | None = None,
) -> AuditEvent:
    return AuditEvent(
        event_id=str(uuid4()),
        timestamp=timestamp or utc_timestamp(),
        ambiente=(ambiente or "").strip(),
        process=(process or "").strip(),
        run_id=(run_id or "").strip(),
        status=validate_audit_status(status),
        name=(name or "").strip(),
        message=(message or "").strip(),
        task=(task or "").strip() or None,
        source_system=(source_system or "").strip() or None,
        execution_id=(execution_id or "").strip() or None,
        details_json=(details_json or "").strip() or None,
    )


def append_audit_event(spark: Any, event: AuditEvent, audit_table: str) -> None:
    df_event = spark.createDataFrame([event.as_dict()])
    (
        df_event.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(audit_table)
    )
