# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Calidad Y Auditoria - Reconciliation Adf
# MAGIC
# MAGIC Ejecuta controles de calidad y reconciliacion para validar la migracion ADF a Databricks.
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
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("quality_table", "")
dbutils.widgets.text("adf_comparison_table", "")
dbutils.widgets.text("tolerance_pct", "0")

from uuid import uuid4

from priorizacion_stock_toledano.quality.quality_rules import (
    append_quality_results,
    has_critical_failures,
    quality_results_table_identifier,
)
from priorizacion_stock_toledano.quality.reconciliation import run_reconciliation

ambiente = dbutils.widgets.get("ambiente").strip()
proceso = dbutils.widgets.get("Proceso").strip() or "Modelo_Priorizacion_Stock"
execution_id = dbutils.widgets.get("execution_id").strip() or str(uuid4())
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or f"toledano_gold_{ambiente}"
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"
quality_table_param = dbutils.widgets.get("quality_table").strip()
adf_comparison_table = dbutils.widgets.get("adf_comparison_table").strip() or None
tolerance_pct = float(dbutils.widgets.get("tolerance_pct").strip() or "0")

databricks_table = f"{catalog_gold}.{schema_atlas}.resultados_indice_priorizacion"
quality_table = quality_table_param or quality_results_table_identifier(catalog_gold, schema_atlas)
results = run_reconciliation(
    spark,
    execution_id=execution_id,
    ambiente=ambiente,
    proceso=proceso,
    databricks_table=databricks_table,
    adf_table=adf_comparison_table,
    tolerance_pct=tolerance_pct,
)
append_quality_results(spark, results, quality_table)

for result in results:
    print(result.as_dict())

if has_critical_failures(results):
    failed = [result.as_dict() for result in results if result.severidad == "CRITICAL" and result.resultado == "FAILED"]
    raise RuntimeError(f"Fallo reconciliacion critica ADF vs Databricks: {failed}")
