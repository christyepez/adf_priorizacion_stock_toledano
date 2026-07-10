# Mapa ADF - Priorizacion de Stock

## 1. Objetivo

Este documento resume los componentes ADF migrados del modelo **Modelo Priorizacion Stock** y su correspondencia con Databricks. La migracion evita depender de IDs de jobs ADF y reemplaza cada actividad por notebooks, modulos Python y jobs definidos en Databricks Asset Bundles.

## 2. Parametros ADF preservados

| Parametro ADF | Uso en Databricks |
|---|---|
| `AñoMesDiaInicial` | Parametro de control para `GetControlCargas` y Lakebase |
| `AñoMesDiaFinal` | Parametro de control para `GetControlCargas` y Lakebase |
| `Proceso` | Valor por defecto `Modelo_Priorizacion_Stock` |
| `SistemaFuente` | Valores principales `SapHana`, `sharepoint` |

## 3. Fuentes y propietarios

| Fuente | PropietarioFuente | Uso |
|---|---|---|
| `SapHana` | `VistasSapHana` | Vistas SAP HANA extraidas hacia Bronze |
| `sharepoint` | `DatosPortalDeInformacion` | Archivos del portal extraidos hacia Bronze |

## 4. Pipelines ADF migrados

| Pipeline ADF | Actividades relevantes | Databricks Job |
|---|---|---|
| `0 orquestador_sap_modelo_priorizacion_stock` | Execute Pipeline SAP, control, dependencias | `job_full_priorizacion_stock` |
| `0 orquestador_sharepoint_modelo_priorizacion_stock` | Execute Pipeline SharePoint, control, dependencias | `job_full_priorizacion_stock` |
| `0 ext_saphana_priorizacion_stock` | Lookup, Filter, ForEach, Copy SAP HANA | `job_ext_saphana_priorizacion_stock` |
| `ext_sharepoint_priorizacion_stock` | Lookup, token, ForEach, Copy SharePoint | `job_ext_sharepoint_priorizacion_stock` |
| `0 bronze_to_silver_sap_modelo_priorizacion_stock` | Databricks notebooks SAP | `job_bronze_to_silver_sap_priorizacion_stock` |
| `0 bronze_to_silver_sharepoint_modelo_priorizacion_stock` | Databricks notebooks SharePoint | `job_bronze_to_silver_sharepoint_priorizacion_stock` |
| `0 actulizar_modelo_creacion_indice_priorizacion` | Databricks job/notebook, Copy SQL, WebActivity | `job_modelo_creacion_indice_priorizacion` |

## 5. Mapeo de actividades ADF

| Tipo ADF | Patron detectado | Implementacion Databricks |
|---|---|---|
| Lookup | `GetControlCargas`, `LK_ControlCargas` | `notebooks/01_control/01_nb_get_control_cargas.py` y `src/.../control/get_control_cargas.py` |
| Filter | Filtrado por `PropietarioFuente` | Filtros PySpark sobre esquema normalizado |
| ForEach | Iteracion por objetos de control | Iteracion secuencial en extractores SAP/SharePoint |
| Copy | SAP HANA a Data Lake, SharePoint a Data Lake, Gold a SQL | Extractores Python y `sql_publisher.py` |
| DatabricksNotebook | Transformaciones y modelo | Notebooks refactorizados en `notebooks/03_bronze_to_silver` y `notebooks/04_modelo` |
| DatabricksJob | Ejecucion de modelo existente | Job bundle `job_modelo_creacion_indice_priorizacion` |
| WebActivity | Notificaciones de error/exito | `notebooks/07_audit/02_nb_notify.py` |

## 6. Notebooks Databricks invocados

| Funcion | Notebook |
|---|---|
| Control SQL Server | `notebooks/01_control/01_nb_get_control_cargas.py` |
| Control Lakebase futuro | `notebooks/01_control/02_nb_get_control_lakebase.py` |
| Extraccion SAP HANA | `notebooks/02_extraccion/01_nb_ext_saphana_priorizacion_stock.py` |
| Extraccion SharePoint | `notebooks/02_extraccion/02_nb_ext_sharepoint_priorizacion_stock.py` |
| Bronze to Silver SAP ofertas | `notebooks/03_bronze_to_silver/sap/01_nb_bronze_to_silver_m_ofertas_doc_ventas.py` |
| Bronze to Silver SAP pedidos | `notebooks/03_bronze_to_silver/sap/02_nb_bronze_to_silver_cv_lo_pedido.py` |
| Bronze to Silver grupos | `notebooks/03_bronze_to_silver/sharepoint/01_nb_bronze_to_silver_grupos_priorizacion.py` |
| Bronze to Silver previas | `notebooks/03_bronze_to_silver/sharepoint/02_nb_bronze_to_silver_priorizaciones_previas.py` |
| Modelo indice | `notebooks/04_modelo/02_nb_creacion_indice_priorizacion.py` |
| Calidad | `notebooks/06_quality/01_nb_quality_checks.py` |
| Reconciliacion | `notebooks/06_quality/02_nb_reconciliation_adf.py` |
| Publicacion SQL | `notebooks/05_publicacion/01_nb_publicar_resultados_sql.py` |
| Auditoria inicio/fin | `notebooks/07_audit/01_nb_audit_start.py`, `notebooks/07_audit/03_nb_audit_finish.py` |
| Notificacion | `notebooks/07_audit/02_nb_notify.py` |

## 7. Riesgos de seguridad mitigados

- No se migran URLs firmadas desde ADF.
- No se hardcodean usuarios, passwords, client secrets ni tokens.
- Los secretos se resuelven con `dbutils.secrets.get`.
- Los YAML contienen nombres de secretos, no valores.
- Los mensajes de error de publicacion SQL sanitizan credenciales.

## 8. Salida final del modelo

La salida Gold se publica hacia SQL Server:

- Schema: `dbo`
- Tabla: `Int_Prioriza_Clientes`
- Columnas publicadas: `cod_cliente`, `prioridad`, `Estatus`, `create_timestamp`
