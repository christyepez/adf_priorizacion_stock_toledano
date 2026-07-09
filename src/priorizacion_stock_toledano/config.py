from dataclasses import dataclass


VALID_CONTROL_MODES = {"get_control_cargas", "lakebase"}


@dataclass(frozen=True)
class BundleConfig:
    ambiente: str
    catalog_bronze: str
    catalog_silver: str
    catalog_gold: str
    schema_sap: str
    schema_sharepoint: str
    schema_atlas: str
    secret_scope: str
    storage_account: str
    modo_control: str


def validate_control_mode(modo_control: str) -> str:
    value = (modo_control or "").strip().lower()
    if value not in VALID_CONTROL_MODES:
        valid = ", ".join(sorted(VALID_CONTROL_MODES))
        raise ValueError(f"modo_control invalido: {modo_control!r}. Valores permitidos: {valid}")
    return value
