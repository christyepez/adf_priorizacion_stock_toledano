from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .quality_rules import CRITICAL_SEVERITY, WARNING_SEVERITY, QualityResult, build_quality_result


@dataclass(frozen=True)
class ReconciliationConfig:
    adf_table: str | None = None
    databricks_table: str | None = None
    tolerance_pct: float = 0.0


def compare_counts(adf_count: int, databricks_count: int, tolerance_pct: float = 0.0) -> tuple[bool, int, str]:
    difference = abs(int(databricks_count) - int(adf_count))
    tolerance_rows = int(abs(int(adf_count)) * float(tolerance_pct))
    passed = difference <= tolerance_rows
    message = (
        f"ADF={adf_count}; Databricks={databricks_count}; "
        f"diferencia={difference}; tolerancia_filas={tolerance_rows}"
    )
    return passed, difference, message


def _safe_table_count(spark: Any, table_name: str) -> int | None:
    if not table_name:
        return None
    try:
        if hasattr(spark, "catalog") and not spark.catalog.tableExists(table_name):
            return None
        return int(spark.table(table_name).count())
    except Exception:
        return None


def run_reconciliation(
    spark: Any,
    *,
    execution_id: str,
    ambiente: str,
    proceso: str,
    databricks_table: str,
    adf_table: str | None = None,
    tolerance_pct: float = 0.0,
) -> list[QualityResult]:
    if not adf_table:
        return [
            build_quality_result(
                execution_id=execution_id,
                ambiente=ambiente,
                proceso=proceso,
                tabla=databricks_table,
                regla="adf_reconciliation_count",
                passed=True,
                cantidad_errores=0,
                severidad=WARNING_SEVERITY,
                mensaje="No se configuro fuente de comparacion ADF; reconciliacion omitida",
            )
        ]

    adf_count = _safe_table_count(spark, adf_table)
    databricks_count = _safe_table_count(spark, databricks_table)
    if adf_count is None:
        return [
            build_quality_result(
                execution_id=execution_id,
                ambiente=ambiente,
                proceso=proceso,
                tabla=adf_table,
                regla="adf_reconciliation_count",
                passed=True,
                cantidad_errores=0,
                severidad=WARNING_SEVERITY,
                mensaje="Fuente de comparacion ADF no existe o no esta disponible; reconciliacion omitida",
            )
        ]
    if databricks_count is None:
        return [
            build_quality_result(
                execution_id=execution_id,
                ambiente=ambiente,
                proceso=proceso,
                tabla=databricks_table,
                regla="adf_reconciliation_count",
                passed=False,
                cantidad_errores=1,
                severidad=CRITICAL_SEVERITY,
                mensaje="Tabla Databricks no existe o no esta disponible para reconciliacion",
            )
        ]

    passed, difference, message = compare_counts(adf_count, databricks_count, tolerance_pct)
    return [
        build_quality_result(
            execution_id=execution_id,
            ambiente=ambiente,
            proceso=proceso,
            tabla=databricks_table,
            regla="adf_reconciliation_count",
            passed=passed,
            cantidad_errores=0 if passed else difference,
            severidad=CRITICAL_SEVERITY,
            mensaje=message,
        )
    ]
