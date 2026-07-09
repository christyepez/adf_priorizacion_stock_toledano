from __future__ import annotations

from dataclasses import dataclass


ALLOWED_ENVIRONMENTS = {"dev", "test", "prd"}

TABLE_KEYS = [
    "TBL_RENTABILIDAD_SKU",
    "TBL_COSTO_SERVIR",
    "TBL_DIM_MATERIALES",
    "TBL_DIM_CLIENTES",
    "TBL_CLIENTES_PRIORIZADOS",
    "TBL_INDICE_PRIORIZACION_ANTERIOR",
    "TBL_OUTPUT_INDICE_PRIORIZACION",
    "TBL_OUTPUT_INDICE_PRIORIZACION_HISTORICO",
]


@dataclass(frozen=True)
class ModelTableConfig:
    ambiente: str
    catalog_silver: str
    catalog_gold: str
    schema_sap: str = "sap"
    schema_sharepoint: str = "sharepoint"
    schema_atlas: str = "atlas"


def validar_ambiente(ambiente: str) -> str:
    value = (ambiente or "").strip().lower()
    if value not in ALLOWED_ENVIRONMENTS:
        allowed = ", ".join(sorted(ALLOWED_ENVIRONMENTS))
        raise ValueError(f"Ambiente invalido: {ambiente!r}. Valores permitidos: {allowed}")
    return value


def _default_catalog_silver(ambiente: str) -> str:
    return f"toledano_silver_{ambiente}"


def _default_catalog_gold(ambiente: str) -> str:
    return f"toledano_gold_{ambiente}"


def obtener_tablas(
    ambiente: str,
    *,
    catalog_silver: str | None = None,
    catalog_gold: str | None = None,
    schema_sap: str = "sap",
    schema_sharepoint: str = "sharepoint",
    schema_atlas: str = "atlas",
) -> dict[str, str]:
    env = validar_ambiente(ambiente)
    silver = (catalog_silver or _default_catalog_silver(env)).strip()
    gold = (catalog_gold or _default_catalog_gold(env)).strip()
    sap = (schema_sap or "sap").strip()
    sharepoint = (schema_sharepoint or "sharepoint").strip()
    atlas = (schema_atlas or "atlas").strip()

    if not silver or not gold:
        raise ValueError("catalog_silver y catalog_gold son obligatorios")

    return {
        "TBL_RENTABILIDAD_SKU": f"{silver}.{sap}.fact_fi_rentabilidad_sku",
        "TBL_COSTO_SERVIR": f"{silver}.{sap}.fact_fi_gastos_cs_cliente",
        "TBL_DIM_MATERIALES": f"{silver}.{sap}.dim_materiales",
        "TBL_DIM_CLIENTES": f"{silver}.{sap}.dim_clientes_direccion",
        "TBL_CLIENTES_PRIORIZADOS": f"{silver}.{sharepoint}.grupos_priorizacion",
        "TBL_INDICE_PRIORIZACION_ANTERIOR": f"{silver}.{sharepoint}.priorizaciones_previas",
        "TBL_OUTPUT_INDICE_PRIORIZACION": f"{gold}.{atlas}.resultados_indice_priorizacion",
        "TBL_OUTPUT_INDICE_PRIORIZACION_HISTORICO": f"{gold}.{atlas}.resultados_indice_priorizacion_hist",
    }
