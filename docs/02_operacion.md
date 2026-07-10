# Operacion

## 1. Jobs disponibles

| Job | Archivo | Uso |
|---|---|---|
| Full end to end | `resources/jobs/01_full.yml` | Ejecucion completa del modelo |
| Extracciones SAP/SharePoint | `resources/jobs/02_extracciones.yml` | Reproceso o prueba de extracciones |
| Transformaciones SAP/SharePoint | `resources/jobs/03_transformaciones.yml` | Reproceso Silver |
| Modelo y publicacion | `resources/jobs/04_modelo_publicacion.yml` | Recalculo del indice y publicacion SQL |

## 2. Parametros operativos

| Parametro | Valor por defecto | Descripcion |
|---|---|---|
| `ambiente` | `${var.ambiente}` | `dev`, `test` o `prod` |
| `Proceso` | `Modelo_Priorizacion_Stock` | Proceso de control |
| `SistemaFuente` | Segun job | `SapHana`, `sharepoint` o `all` |
| `AñoMesDiaInicial` | `0` | Fecha inicial de control |
| `AñoMesDiaFinal` | `0` | Fecha final de control |
| `modo_control` | `get_control_cargas` | `get_control_cargas` o `lakebase` |
| `modo_ejecucion` | `normal` | Bandera operacional para extensiones |
| `emails` | vacio | Lista de destinatarios separados por coma o punto y coma |

## 3. Ejecucion por ambiente

Validar el bundle:

```bash
databricks bundle validate -t dev
databricks bundle validate -t test
databricks bundle validate -t prod
```

Desplegar:

```bash
databricks bundle deploy -t dev
databricks bundle deploy -t test
databricks bundle deploy -t prod
```

Ejecutar job full:

```bash
databricks bundle run job_full_priorizacion_stock -t dev
```

Para `test` y `prod`, cambiar el target correspondiente.

## 4. Flujo operativo end to end

El job `job_full_priorizacion_stock` ejecuta:

1. `audit_start`
2. `ext_saphana_priorizacion_stock`
3. `ext_sharepoint_priorizacion_stock`
4. `bronze_to_silver_sap`
5. `bronze_to_silver_sap_cv_lo_pedido`
6. `bronze_to_silver_sharepoint`
7. `bronze_to_silver_sharepoint_priorizaciones_previas`
8. `modelo_creacion_indice_priorizacion`
9. `quality_checks`
10. `reconciliation_adf`
11. `publicar_resultados_sql`
12. `notify_success`
13. `audit_finish`

## 5. Auditoria

Los eventos operativos se guardan en:

```text
toledano_gold_{ambiente}.atlas.audit_pipeline_events
```

Estados soportados:

- `STARTED`
- `SUCCEEDED`
- `FAILED`
- `WARNING`

## 6. Calidad

Los resultados de calidad se guardan en:

```text
toledano_gold_{ambiente}.atlas.quality_results
```

Si una regla con severidad `CRITICAL` falla, el notebook de calidad o reconciliacion levanta una excepcion controlada y el job se detiene.

## 7. Publicacion SQL Server

El notebook `notebooks/05_publicacion/01_sql_server.py` lee:

```sql
SELECT cod_cliente, prioridad, Estatus, create_timestamp
FROM toledano_gold_{ambiente}.atlas.resultados_indice_priorizacion
```

Y escribe por JDBC en:

```text
dbo.Int_Prioriza_Clientes
```

Modos soportados:

- `append`
- `overwrite`
- `truncate_insert`

El modo configurado en jobs es `truncate_insert`.

## 8. Manejo de incidentes

| Sintoma | Accion recomendada |
|---|---|
| Falla control de cargas | Validar Secret Scope SQL control y salida de `conf.GetControlCargas` |
| Falla SAP HANA | Revisar driver, secretos SAP y query generada por objeto |
| Falla SharePoint | Revisar secretos OAuth/token, permisos Graph y ruta de archivo |
| Falla Bronze to Silver | Revisar columnas obligatorias y cambios de schema |
| Falla modelo | Revisar disponibilidad de tablas Silver y columnas del modelo |
| Falla calidad | Consultar `quality_results` por `execution_id` |
| Falla publicacion SQL | Validar `sql_publication_server`, `sql_publication_database`, secretos de usuario/password, permisos y tabla destino |
| Falla notificacion | Consultar evento `WARNING` en auditoria; no debe ocultar el error original |

## 9. Reprocesos

Para reprocesar parcialmente:

- Extraccion SAP: ejecutar `job_ext_saphana_priorizacion_stock`.
- Extraccion SharePoint: ejecutar `job_ext_sharepoint_priorizacion_stock`.
- Transformacion SAP: ejecutar `job_bronze_to_silver_sap_priorizacion_stock`.
- Transformacion SharePoint: ejecutar `job_bronze_to_silver_sharepoint_priorizacion_stock`.
- Modelo y publicacion: ejecutar `job_modelo_creacion_indice_priorizacion`.

Cada reproceso debe usar un `execution_id` trazable, preferiblemente el `job.run_id`.
