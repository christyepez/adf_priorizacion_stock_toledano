from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


QUALITY_RESULTS_TABLE = "quality_results"
CRITICAL_SEVERITY = "CRITICAL"
WARNING_SEVERITY = "WARNING"
RESULT_PASSED = "PASSED"
RESULT_FAILED = "FAILED"

QUALITY_COLUMNS = [
    "execution_id",
    "ambiente",
    "proceso",
    "tabla",
    "regla",
    "resultado",
    "cantidad_errores",
    "severidad",
    "mensaje",
    "timestamp",
]


@dataclass(frozen=True)
class QualityResult:
    execution_id: str
    ambiente: str
    proceso: str
    tabla: str
    regla: str
    resultado: str
    cantidad_errores: int
    severidad: str
    mensaje: str
    timestamp: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def quality_results_table_identifier(catalog_gold: str, schema_atlas: str, table_name: str = QUALITY_RESULTS_TABLE) -> str:
    catalog = (catalog_gold or "").strip()
    schema = (schema_atlas or "").strip()
    table = (table_name or "").strip()
    if not catalog or not schema or not table:
        raise ValueError("catalog_gold, schema_atlas y table_name son obligatorios")
    return f"{catalog}.{schema}.{table}"


def build_quality_result(
    *,
    execution_id: str,
    ambiente: str,
    proceso: str,
    tabla: str,
    regla: str,
    passed: bool,
    cantidad_errores: int,
    severidad: str,
    mensaje: str,
    timestamp: str | None = None,
) -> QualityResult:
    return QualityResult(
        execution_id=execution_id,
        ambiente=ambiente,
        proceso=proceso,
        tabla=tabla,
        regla=regla,
        resultado=RESULT_PASSED if passed else RESULT_FAILED,
        cantidad_errores=int(cantidad_errores or 0),
        severidad=(severidad or WARNING_SEVERITY).upper(),
        mensaje=mensaje,
        timestamp=timestamp or utc_timestamp(),
    )


def build_default_quality_tables(
    *,
    catalog_bronze: str,
    catalog_silver: str,
    catalog_gold: str,
    schema_sap: str,
    schema_sharepoint: str,
    schema_atlas: str,
) -> dict[str, list[str] | str]:
    return {
        "bronze_sap": [
            f"{catalog_bronze}.{schema_sap}.fact_cv_m_ofertas_doc_ventas",
            f"{catalog_bronze}.{schema_sap}.fact_cv_lo_pedido",
        ],
        "bronze_sharepoint": [
            f"{catalog_bronze}.{schema_sharepoint}.grupos_priorizacion",
            f"{catalog_bronze}.{schema_sharepoint}.priorizaciones_previas",
        ],
        "silver": [
            f"{catalog_silver}.{schema_sap}.fact_cv_m_ofertas_doc_ventas",
            f"{catalog_silver}.{schema_sap}.fact_cv_lo_pedido",
            f"{catalog_silver}.{schema_sharepoint}.grupos_priorizacion",
            f"{catalog_silver}.{schema_sharepoint}.priorizaciones_previas",
        ],
        "gold": f"{catalog_gold}.{schema_atlas}.resultados_indice_priorizacion",
    }


def table_exists(spark: Any, table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        try:
            spark.table(table_name).limit(0).count()
            return True
        except Exception:
            return False


def table_count(spark: Any, table_name: str) -> int:
    return int(spark.table(table_name).count())


def _result_for_table_count(
    spark: Any,
    *,
    table_name: str,
    rule_name: str,
    execution_id: str,
    ambiente: str,
    proceso: str,
    severity: str = CRITICAL_SEVERITY,
) -> QualityResult:
    if not table_exists(spark, table_name):
        return build_quality_result(
            execution_id=execution_id,
            ambiente=ambiente,
            proceso=proceso,
            tabla=table_name,
            regla=rule_name,
            passed=False,
            cantidad_errores=1,
            severidad=severity,
            mensaje=f"No existe la tabla {table_name}",
        )

    count = table_count(spark, table_name)
    return build_quality_result(
        execution_id=execution_id,
        ambiente=ambiente,
        proceso=proceso,
        tabla=table_name,
        regla=rule_name,
        passed=count > 0,
        cantidad_errores=0 if count > 0 else 1,
        severidad=severity,
        mensaje=f"Conteo de registros: {count}",
    )


def critical_null_count(df: Any, columns: list[str]) -> int:
    from pyspark.sql.functions import col

    condition = None
    for column_name in columns:
        current = col(column_name).isNull()
        condition = current if condition is None else condition | current
    if condition is None:
        return 0
    return int(df.filter(condition).count())


def duplicate_key_count(df: Any, key_column: str) -> int:
    from pyspark.sql.functions import col, count

    return int(df.groupBy(key_column).agg(count("*").alias("_count")).filter(col("_count") > 1).count())


def run_quality_checks(
    spark: Any,
    *,
    execution_id: str,
    ambiente: str,
    proceso: str,
    catalog_bronze: str,
    catalog_silver: str,
    catalog_gold: str,
    schema_sap: str = "sap",
    schema_sharepoint: str = "sharepoint",
    schema_atlas: str = "atlas",
) -> list[QualityResult]:
    tables = build_default_quality_tables(
        catalog_bronze=catalog_bronze,
        catalog_silver=catalog_silver,
        catalog_gold=catalog_gold,
        schema_sap=schema_sap,
        schema_sharepoint=schema_sharepoint,
        schema_atlas=schema_atlas,
    )
    results: list[QualityResult] = []

    for table_name in tables["bronze_sap"]:
        results.append(
            _result_for_table_count(
                spark,
                table_name=table_name,
                rule_name="bronze_sap_has_data",
                execution_id=execution_id,
                ambiente=ambiente,
                proceso=proceso,
            )
        )

    for table_name in tables["bronze_sharepoint"]:
        results.append(
            _result_for_table_count(
                spark,
                table_name=table_name,
                rule_name="bronze_sharepoint_has_data",
                execution_id=execution_id,
                ambiente=ambiente,
                proceso=proceso,
            )
        )

    for table_name in tables["silver"]:
        results.append(
            _result_for_table_count(
                spark,
                table_name=table_name,
                rule_name="silver_has_data",
                execution_id=execution_id,
                ambiente=ambiente,
                proceso=proceso,
            )
        )

    gold_table = str(tables["gold"])
    results.append(
        _result_for_table_count(
            spark,
            table_name=gold_table,
            rule_name="gold_has_data",
            execution_id=execution_id,
            ambiente=ambiente,
            proceso=proceso,
        )
    )

    if table_exists(spark, gold_table):
        df_gold = spark.table(gold_table)
        missing_columns = [column for column in ["cod_cliente", "prioridad", "Estatus"] if column not in df_gold.columns]
        if missing_columns:
            results.append(
                build_quality_result(
                    execution_id=execution_id,
                    ambiente=ambiente,
                    proceso=proceso,
                    tabla=gold_table,
                    regla="gold_required_columns",
                    passed=False,
                    cantidad_errores=len(missing_columns),
                    severidad=CRITICAL_SEVERITY,
                    mensaje=f"Faltan columnas criticas: {', '.join(missing_columns)}",
                )
            )
        else:
            null_count = critical_null_count(df_gold, ["cod_cliente", "prioridad", "Estatus"])
            results.append(
                build_quality_result(
                    execution_id=execution_id,
                    ambiente=ambiente,
                    proceso=proceso,
                    tabla=gold_table,
                    regla="gold_critical_nulls",
                    passed=null_count == 0,
                    cantidad_errores=null_count,
                    severidad=CRITICAL_SEVERITY,
                    mensaje=f"Nulos criticos en cod_cliente/prioridad/Estatus: {null_count}",
                )
            )

            duplicate_count = duplicate_key_count(df_gold, "cod_cliente")
            results.append(
                build_quality_result(
                    execution_id=execution_id,
                    ambiente=ambiente,
                    proceso=proceso,
                    tabla=gold_table,
                    regla="gold_duplicate_cod_cliente",
                    passed=duplicate_count == 0,
                    cantidad_errores=duplicate_count,
                    severidad=CRITICAL_SEVERITY,
                    mensaje=f"Clientes duplicados en Gold: {duplicate_count}",
                )
            )

    return results


def append_quality_results(spark: Any, results: list[QualityResult], quality_table: str) -> None:
    if not results:
        return
    rows = [result.as_dict() for result in results]
    df_results = spark.createDataFrame(rows).select(*QUALITY_COLUMNS)
    (
        df_results.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quality_table)
    )


def has_critical_failures(results: list[QualityResult]) -> bool:
    return any(result.severidad == CRITICAL_SEVERITY and result.resultado == RESULT_FAILED for result in results)
