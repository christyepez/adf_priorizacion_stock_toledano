# Pruebas y validacion

## 1. Estrategia de pruebas

La estrategia cubre pruebas unitarias, validacion de bundle, pruebas funcionales por capa, reconciliacion ADF vs Databricks y controles de seguridad.

## 2. Pruebas unitarias

Las pruebas se ubican en `tests/` y cubren:

| Archivo | Cobertura |
|---|---|
| `test_control.py` | Query `GetControlCargas`, normalizacion ARM, filtros de propietario |
| `test_lakebase_control.py` | Query Lakebase, Secret Scope, URL JDBC PostgreSQL sin credenciales |
| `test_saphana_query_builder.py` | Construccion de queries SAP HANA |
| `test_sharepoint_mapping.py` | Mapeo SharePoint y rutas destino |
| `test_sap_bronze_to_silver_transformations.py` | Limpieza/casts SAP |
| `test_sharepoint_bronze_to_silver_transformations.py` | Limpieza/casts SharePoint |
| `test_model_parameters.py` | Resolucion de tablas por ambiente |
| `test_indice_priorizacion.py` | Funciones principales del modelo |
| `test_publication_sql.py` | Publicacion SQL, modos y sanitizacion |
| `test_audit_notifier.py` | Payload de notificacion y eventos |
| `test_quality_rules.py` | Calidad y reconciliacion |

## 3. Validaciones de calidad implementadas

El notebook `notebooks/06_quality/nb_quality_checks.py` valida:

1. Existen datos en Bronze SAP.
2. Existen datos en Bronze SharePoint.
3. Existen datos en Silver:
   - `sap.fact_cv_m_ofertas_doc_ventas`
   - `sap.fact_cv_lo_pedido`
   - `sharepoint.grupos_priorizacion`
   - `sharepoint.priorizaciones_previas`
4. Existen datos en Gold:
   - `atlas.resultados_indice_priorizacion`
5. Nulos criticos:
   - `cod_cliente`
   - `prioridad`
   - `Estatus`
6. Duplicados por `cod_cliente` en Gold.

## 4. Tabla de resultados de calidad

La tabla:

```text
toledano_gold_{ambiente}.atlas.quality_results
```

contiene:

- `execution_id`
- `ambiente`
- `proceso`
- `tabla`
- `regla`
- `resultado`
- `cantidad_errores`
- `severidad`
- `mensaje`
- `timestamp`

## 5. Reconciliacion ADF vs Databricks

`notebooks/06_quality/nb_reconciliation_adf.py` compara conteos si se configura `adf_comparison_table`.

Comportamiento:

- Si no existe fuente ADF, registra resultado `PASSED` con severidad `WARNING` y mensaje de omision.
- Si existe fuente ADF, compara conteos contra Gold Databricks.
- Si la diferencia supera `tolerance_pct`, registra fallo critico.

## 6. Pruebas funcionales recomendadas

### Control

- Ejecutar `nb_get_control_cargas.py` con `SistemaFuente=SapHana`.
- Validar que la vista temporal `vw_control_cargas_priorizacion_stock` tenga registros activos.
- Repetir con `SistemaFuente=sharepoint`.

### Extraccion SAP

- Validar cantidad de objetos procesados.
- Revisar metricas `rows_read`, `rows_written`, `status`, `error_message`.
- Confirmar columnas tecnicas: `ingestion_timestamp`, `source_system`, `process_name`, `execution_id`, `source_object`.

### Extraccion SharePoint

- Validar bytes leidos por archivo.
- Confirmar escritura en Bronze.
- Revisar que no se usen URLs firmadas desde ADF.

### Bronze to Silver

- Comparar conteos Bronze vs Silver cuando aplique.
- Validar columnas esperadas y tipos.
- Revisar `execution_id`.

### Modelo

- Confirmar existencia de:
  - `toledano_gold_{ambiente}.atlas.resultados_indice_priorizacion`
  - `toledano_gold_{ambiente}.atlas.resultados_indice_priorizacion_hist`
- Validar que `cod_cliente`, `prioridad` y `Estatus` no sean nulos.

### Publicacion SQL

- Validar filas publicadas en `dbo.Int_Prioriza_Clientes`.
- Confirmar que el modo `truncate_insert` conserva estructura destino.

## 7. Controles de seguridad

Se debe revisar que:

- No existan passwords ni tokens en notebooks, YAML o SQL.
- Los Secret Scope existan para cada ambiente.
- Los nombres de secretos del bundle correspondan a Key Vault/Databricks Secrets.
- Los logs no impriman valores sensibles.

## 8. Criterios de aprobacion

- Bundle validado en el ambiente.
- Job full ejecutado correctamente.
- Sin reglas criticas fallidas.
- Conteos reconciliados con ADF o desviacion aprobada.
- Resultado publicado en SQL Server.
- Auditoria y notificacion registradas.
