# Estrategia de implementacion

## 1. Enfoque

La migracion se plantea como una industrializacion incremental del modelo **Modelo_Priorizacion_Stock**, reemplazando actividades ADF por tasks Databricks administradas por Databricks Asset Bundles. El objetivo no es reescribir la logica de negocio, sino trasladarla a una arquitectura repetible, auditable y extensible.

## 2. Fases de implementacion

### Fase 1 - Inventario y trazabilidad ADF

Se identifican pipelines, actividades, parametros, datasets, linked services, notebooks y riesgos de seguridad del ARM. La salida de esta fase es el mapa ADF a Databricks y la matriz de equivalencias.

Pipelines migrados:

- `0 orquestador_sap_modelo_priorizacion_stock`
- `0 orquestador_sharepoint_modelo_priorizacion_stock`
- `0 ext_saphana_priorizacion_stock`
- `ext_sharepoint_priorizacion_stock`
- `0 bronze_to_silver_sap_modelo_priorizacion_stock`
- `0 bronze_to_silver_sharepoint_modelo_priorizacion_stock`
- `0 actulizar_modelo_creacion_indice_priorizacion`

### Fase 2 - Base Databricks Asset Bundle

Se crea la estructura:

- `databricks.yml`
- `resources/jobs`
- `notebooks`
- `src/priorizacion_stock_toledano`
- `sql`
- `tests`
- `docs`

El bundle define targets `dev`, `test` y `prod`, y variables para catalogos, schemas, Secret Scope, storage, SQL Server, notificacion y modo de control.

### Fase 3 - Control de cargas

Se implementa `nb_get_control_cargas.py` y `get_control_cargas.py` para reemplazar los Lookup ADF `GetControlCargas` y `LK_ControlCargas`.

El contrato normalizado es:

- `proceso`
- `sistema_fuente`
- `propietario_fuente`
- `columnas_archivo_fuente`
- `ruta_archivo_fuente`
- `nombre_archivo_fuente`
- `filtros_archivo_fuente`
- `ruta_archivo_destino`
- `nombre_archivo_destino`
- `extension_archivo_destino`
- `tipo_carga`
- `activo`
- `orden_ejecucion`

Este contrato permite que SAP, SharePoint y una futura fuente Lakebase trabajen sin modificar el resto del pipeline.

### Fase 4 - Extracciones

SAP HANA:

- Reemplaza `0 ext_saphana_priorizacion_stock`.
- Ejecuta control de cargas.
- Filtra `PropietarioFuente = VistasSapHana`.
- Construye query con el patron `SELECT columnas FROM "schema"."objeto" filtros`.
- Escribe Bronze con columnas tecnicas y metricas por objeto.

SharePoint:

- Reemplaza `ext_sharepoint_priorizacion_stock`.
- Ejecuta control de cargas con `SistemaFuente = sharepoint`.
- Filtra `PropietarioFuente = DatosPortalDeInformacion`.
- Obtiene endpoint/token mediante Secret Scope.
- Copia archivos binarios/Excel/CSV hacia Bronze.

### Fase 5 - Bronze to Silver

Se refactorizan notebooks originales y se crean versiones Python:

- `nb_bronze_to_silver_m_ofertas_doc_ventas.py`
- `nb_bronze_to_silver_cv_lo_pedido.py`
- `nb_bronze_to_silver_grupos_priorizacion.py`
- `nb_bronze_to_silver_priorizaciones_previas.py`

Las funciones reutilizables viven en:

- `src/priorizacion_stock_toledano/transformations/sap_bronze_to_silver.py`
- `src/priorizacion_stock_toledano/transformations/sharepoint_bronze_to_silver.py`

### Fase 6 - Modelo de indice

Se refactoriza `01_nb_creacion_indice_refac_v03.ipynb` en:

- `notebooks/04_modelo/nb_creacion_indice_priorizacion.py`
- `src/priorizacion_stock_toledano/model/indice_priorizacion.py`

El notebook queda como orquestador y el modulo conserva funciones principales:

- `calcular_margen_acumulado`
- `crear_indice_priorizacion`
- `crear_score_priorizacion`
- `calcular_margen_porcentual_promedio_mensual`

### Fase 7 - Calidad, reconciliacion y publicacion

Se agregan:

- `notebooks/06_quality/nb_quality_checks.py`
- `notebooks/06_quality/nb_reconciliation_adf.py`
- `notebooks/05_publicacion/nb_publicar_resultados_sql.py`

El job falla controladamente ante reglas criticas.

### Fase 8 - Auditoria y notificaciones

Se reemplazan WebActivity ADF por:

- `notebooks/07_audit/nb_audit_start.py`
- `notebooks/07_audit/nb_audit_finish.py`
- `notebooks/07_audit/nb_notify.py`

Los eventos se registran en Delta y las notificaciones usan endpoint desde Secret Scope.

### Fase 9 - Evolucion Lakebase

Se disena el modelo futuro PostgreSQL en `sql/lakebase`. Lakebase puede reemplazar gradualmente `GetControlCargas` usando `modo_control = lakebase`, manteniendo el mismo esquema normalizado.

## 3. Criterios de aceptacion

- El bundle despliega jobs en `dev`, `test` y `prod`.
- No existen secretos ni URLs firmadas en repositorio.
- El job full ejecuta de punta a punta.
- Las tablas Gold y SQL Server contienen registros esperados.
- Las reglas de calidad criticas pasan.
- Existe evidencia en tablas de auditoria y calidad.
- Lakebase queda disenado sin afectar la ejecucion actual.

## 4. Estrategia de cutover

1. Ejecutar en `dev` con fuentes de prueba.
2. Ejecutar en `test` en paralelo contra ADF.
3. Comparar conteos y resultados mediante `nb_reconciliation_adf.py`.
4. Validar publicacion SQL en tabla controlada o ambiente no productivo.
5. Congelar cambios funcionales durante ventana de cutover.
6. Activar job Databricks productivo.
7. Mantener ADF en modo rollback temporal hasta cierre de estabilizacion.
