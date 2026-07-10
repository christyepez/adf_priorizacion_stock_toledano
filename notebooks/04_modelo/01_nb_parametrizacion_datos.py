# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Modelo De Priorizacion - Parametrizacion Datos
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
define_text_widget(dbutils, "catalog_silver", "")
define_text_widget(dbutils, "catalog_gold", "")
define_text_widget(dbutils, "schema_sap", "sap")
define_text_widget(dbutils, "schema_sharepoint", "sharepoint")
define_text_widget(dbutils, "schema_atlas", "atlas")

from priorizacion_stock_toledano.model.model_parameters import obtener_tablas

ambiente = dbutils.widgets.get("ambiente")
catalog_silver = dbutils.widgets.get("catalog_silver").strip() or None
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or None
schema_sap = dbutils.widgets.get("schema_sap").strip() or "sap"
schema_sharepoint = dbutils.widgets.get("schema_sharepoint").strip() or "sharepoint"
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"

tablas_modelo = obtener_tablas(
    ambiente,
    catalog_silver=catalog_silver,
    catalog_gold=catalog_gold,
    schema_sap=schema_sap,
    schema_sharepoint=schema_sharepoint,
    schema_atlas=schema_atlas,
)
