# Evolucion futura hacia Lakebase PostgreSQL

## 1. Objetivo

Lakebase PostgreSQL se propone como reemplazo gradual de `conf.GetControlCargas`, manteniendo el mismo contrato normalizado que ya consumen los extractores SAP HANA y SharePoint. La evolucion no debe requerir cambios en transformaciones, modelo, calidad ni publicacion.

## 2. Artefactos creados

| Archivo | Proposito |
|---|---|
| `sql/lakebase/001_create_control_tables.sql` | Crea schema y tablas de control |
| `sql/lakebase/002_insert_priorizacion_stock_sample_data.sql` | Inserta configuracion ejemplo para Priorizacion de Stock |
| `sql/lakebase/003_create_control_views.sql` | Crea vistas normalizadas y compatibles con ADF |
| `src/priorizacion_stock_toledano/control/lakebase_control.py` | Lector JDBC Lakebase |
| `notebooks/01_control/nb_get_control_lakebase.py` | Notebook Databricks equivalente a `GetControlCargas` |

## 3. Modelo de datos Lakebase

Tablas:

- `control.proceso_control`
- `control.sistema_fuente_control`
- `control.propietario_fuente_control`
- `control.control_cargas`

Vista principal:

```text
control.vw_control_cargas_priorizacion_stock
```

Vista compatible:

```text
control.vw_get_control_cargas_compat
```

## 4. Contrato normalizado

Lakebase devuelve:

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

Este contrato es igual al usado por `GetControlCargas`.

## 5. Configuracion soportada

| Proceso | SistemaFuente | PropietarioFuente |
|---|---|---|
| `Modelo_Priorizacion_Stock` | `SapHana` | `VistasSapHana` |
| `Modelo_Priorizacion_Stock` | `sharepoint` | `DatosPortalDeInformacion` |

## 6. Seguridad

Lakebase se consume por JDBC PostgreSQL. Los datos sensibles se leen desde Secret Scope:

- `lakebase_host_secret`
- `lakebase_port_secret`
- `lakebase_database_secret`
- `lakebase_username_secret`
- `lakebase_password_secret`

El JDBC URL no contiene credenciales.

## 7. Modo de adopcion

El bundle ya contempla `modo_control` con valores:

- `get_control_cargas`
- `lakebase`

Estrategia recomendada:

1. Crear tablas Lakebase en ambiente `dev`.
2. Poblar datos desde `GetControlCargas` productivo o desde una exportacion aprobada.
3. Ejecutar `nb_get_control_lakebase.py` y comparar salida con `nb_get_control_cargas.py`.
4. Validar SAP HANA y SharePoint usando el control Lakebase en `dev`.
5. Repetir en `test` con ejecuciones paralelas.
6. Activar `modo_control=lakebase` para pipelines seleccionados.
7. Mantener `GetControlCargas` como fallback durante estabilizacion.

## 8. Reglas de gobierno

- Toda nueva fuente debe registrarse en Lakebase con `activo=false` inicialmente.
- `orden_ejecucion` debe ser explicito.
- Los cambios deben versionarse con scripts SQL incrementales.
- No se deben almacenar secretos, endpoints firmados ni tokens.
- Las vistas deben preservar compatibilidad con el esquema normalizado.

## 9. Beneficios esperados

- Menor acoplamiento con SQL Server de control legado.
- Administracion declarativa de fuentes.
- Posibilidad de versionar y auditar cambios de control.
- Compatibilidad natural con Databricks y gobierno de datos.
