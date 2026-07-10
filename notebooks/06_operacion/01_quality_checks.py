# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Calidad Y Auditoria - Quality Checks
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
define_text_widget(dbutils, "execution_id", "")
define_text_widget(dbutils, "catalog_bronze", "")
define_text_widget(dbutils, "catalog_silver", "")
define_text_widget(dbutils, "catalog_gold", "")
define_text_widget(dbutils, "schema_sap", "sap")
define_text_widget(dbutils, "schema_sharepoint", "sharepoint")
define_text_widget(dbutils, "schema_atlas", "atlas")
define_text_widget(dbutils, "quality_table", "")

from uuid import uuid4

from priorizacion_stock_toledano.quality.quality_rules import (
    append_quality_results,
    has_critical_failures,
    quality_results_table_identifier,
    run_quality_checks,
)

ambiente = dbutils.widgets.get("ambiente").strip()
proceso = dbutils.widgets.get("Proceso").strip() or "Modelo_Priorizacion_Stock"
execution_id = dbutils.widgets.get("execution_id").strip() or str(uuid4())
catalog_bronze = dbutils.widgets.get("catalog_bronze").strip() or f"toledano_bronze_{ambiente}"
catalog_silver = dbutils.widgets.get("catalog_silver").strip() or f"toledano_silver_{ambiente}"
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or f"toledano_gold_{ambiente}"
schema_sap = dbutils.widgets.get("schema_sap").strip() or "sap"
schema_sharepoint = dbutils.widgets.get("schema_sharepoint").strip() or "sharepoint"
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"
quality_table_param = dbutils.widgets.get("quality_table").strip()

quality_table = quality_table_param or quality_results_table_identifier(catalog_gold, schema_atlas)
results = run_quality_checks(
    spark,
    execution_id=execution_id,
    ambiente=ambiente,
    proceso=proceso,
    catalog_bronze=catalog_bronze,
    catalog_silver=catalog_silver,
    catalog_gold=catalog_gold,
    schema_sap=schema_sap,
    schema_sharepoint=schema_sharepoint,
    schema_atlas=schema_atlas,
)
append_quality_results(spark, results, quality_table)

for result in results:
    print(result.as_dict())

if has_critical_failures(results):
    failed = [result.as_dict() for result in results if result.severidad == "CRITICAL" and result.resultado == "FAILED"]
    raise RuntimeError(f"Fallaron reglas criticas de calidad: {failed}")
