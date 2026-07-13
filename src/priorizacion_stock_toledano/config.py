from dataclasses import dataclass
from typing import Any


VALID_CONTROL_MODES = {"get_control_cargas", "lakebase"}


# Defaults equivalentes al target dev de databricks.yml.
# Los jobs del bundle siguen siendo la fuente de verdad en ejecuciones productivas;
# estos valores evitan widgets vacios cuando un notebook se ejecuta manualmente.
DEFAULT_WIDGET_VALUES = {
    "ambiente": "dev",
    "Proceso": "Modelo_Priorizacion_Stock",
    "SistemaFuente": "all",
    "AñoMesDiaInicial": "0",
    "AñoMesDiaFinal": "0",
    "modo_control": "get_control_cargas",
    "modo_ejecucion": "normal",
    "secret_scope": "sc-kv-toledano-bigdata-dev",
    "catalog_bronze": "toledano_bronze_dev",
    "catalog_silver": "toledano_silver_dev",
    "catalog_gold": "toledano_gold_dev",
    "schema_sap": "sap",
    "schema_sharepoint": "sharepoint",
    "schema_atlas": "atlas",
    "schema_audit": "audit",
    "storage_account_name": "dlsbigdatatoledanodev",
    "storage_account_url": "https://dlsbigdatatoledanodev.dfs.core.windows.net/",
    "storage_account_key_secret": "sc-dlsbigdatatoledano-key",
    "storage_account_url_secret": "sc-dlsbigdatatoledano-url",
    "bronze_container": "bronze",
    "silver_container": "silver",
    "gold_container": "gold",
    "staging_path": "bronze/temp",
    "sql_control_server": "",
    "sql_control_database": "",
    "sql_control_schema": "conf",
    "sql_control_table": "ControlCargas",
    "sql_control_read_mode": "jdbc_table",
    "sql_control_spark_sql": "",
    "sql_control_spark_relation": "",
    "sql_control_server_secret": "sc-sqlbigdatatoledano-server",
    "sql_control_database_secret": "sc-sqlbigdatatoledano-database",
    "sql_control_username_secret": "sc-sqlbigdatatoledano-username",
    "sql_control_password_secret": "sc-sqlbigdatatoledano-password",
    "sql_control_encrypt": "true",
    "sql_control_trust_server_certificate": "false",
    "sql_publication_server": "PTSVR2003",
    "sql_publication_database": "ORC_SAP",
    "sql_publication_username_secret": "sc-sql-orcpanama-username",
    "sql_publication_password_secret": "sc-sql-orcpanama-password",
    "sql_publication_encrypt": "true",
    "sql_publication_trust_server_certificate": "true",
    "target_schema": "dbo",
    "target_table": "Int_Prioriza_Clientes",
    "modo_publicacion": "truncate_insert",
    "sap_hana_server_secret": "sc-saphana-servernode",
    "sap_hana_port_secret": "",
    "sap_hana_username_secret": "sc-saphana-username",
    "sap_hana_password_secret": "sc-saphana-password",
    "sap_hana_driver": "com.sap.db.jdbc.Driver",
    "sap_hana_propietario_fuente": "VistasSapHana",
    "sharepoint_base_url": "https://pronaca365.sharepoint.com/",
    "sharepoint_auth_mode": "graph_client_credentials",
    "sharepoint_connection_path": "",
    "sharepoint_token_secret": "",
    "sharepoint_client_id_secret": "sc-sharepoint-client-id",
    "sharepoint_client_secret_secret": "sc-sharepoint-secret-id",
    "sharepoint_tenant_id_secret": "sc-sharepoint-tenant-id",
    "sharepoint_site_id_secret": "",
    "sharepoint_drive_id_secret": "",
    "sharepoint_propietario_fuente": "DatosPortalDeInformacion",
    "notification_endpoint_secret": "",
    "notification_enabled": "false",
    "emails": "",
    "quality_table": "",
    "audit_table": "",
    "audit_delta_enabled": "false",
    "audit_delta_table": "",
    "metrics_delta_table": "",
    "adf_comparison_table": "",
    "tolerance_pct": "0",
}


BUNDLE_ALIGNED_WIDGETS = {
    "secret_scope",
    "catalog_bronze",
    "catalog_silver",
    "catalog_gold",
    "schema_sap",
    "schema_sharepoint",
    "schema_atlas",
    "schema_audit",
    "storage_account_name",
    "storage_account_url",
    "storage_account_key_secret",
    "storage_account_url_secret",
    "bronze_container",
    "silver_container",
    "gold_container",
    "staging_path",
    "sql_control_server",
    "sql_control_database",
    "sql_control_schema",
    "sql_control_table",
    "sql_control_read_mode",
    "sql_control_spark_sql",
    "sql_control_spark_relation",
    "sql_control_server_secret",
    "sql_control_database_secret",
    "sql_control_username_secret",
    "sql_control_password_secret",
    "sql_control_encrypt",
    "sql_control_trust_server_certificate",
    "sql_publication_server",
    "sql_publication_database",
    "sql_publication_username_secret",
    "sql_publication_password_secret",
    "sql_publication_encrypt",
    "sql_publication_trust_server_certificate",
    "modo_publicacion",
    "sap_hana_server_secret",
    "sap_hana_port_secret",
    "sap_hana_username_secret",
    "sap_hana_password_secret",
    "sap_hana_driver",
    "sap_hana_propietario_fuente",
    "sharepoint_base_url",
    "sharepoint_auth_mode",
    "sharepoint_connection_path",
    "sharepoint_token_secret",
    "sharepoint_client_id_secret",
    "sharepoint_client_secret_secret",
    "sharepoint_tenant_id_secret",
    "sharepoint_site_id_secret",
    "sharepoint_drive_id_secret",
    "sharepoint_propietario_fuente",
    "notification_endpoint_secret",
    "notification_enabled",
}


@dataclass(frozen=True)
class BundleConfig:
    ambiente: str
    catalog_bronze: str
    catalog_silver: str
    catalog_gold: str
    schema_sap: str
    schema_sharepoint: str
    schema_atlas: str
    schema_audit: str
    secret_scope: str
    storage_account_name: str
    modo_control: str
    modo_ejecucion: str = "normal"


def validate_control_mode(modo_control: str) -> str:
    value = (modo_control or "").strip().lower()
    if value not in VALID_CONTROL_MODES:
        valid = ", ".join(sorted(VALID_CONTROL_MODES))
        raise ValueError(f"modo_control invalido: {modo_control!r}. Valores permitidos: {valid}")
    return value


def widget_default(name: str, fallback: str = "") -> str:
    """Return a safe notebook default aligned with databricks.yml dev target."""
    if name in BUNDLE_ALIGNED_WIDGETS:
        return DEFAULT_WIDGET_VALUES.get(name, fallback)
    if fallback != "":
        return fallback
    return DEFAULT_WIDGET_VALUES.get(name, fallback)


def define_text_widget(dbutils: Any, name: str, fallback: str = "") -> None:
    """Create a text widget using bundle-aligned defaults for manual notebook runs."""
    dbutils.widgets.text(name, widget_default(name, fallback))
