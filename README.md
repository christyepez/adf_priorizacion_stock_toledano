# priorizacion-stock-toledano

Databricks Asset Bundle base para migrar y operar el modelo de Priorizacion de Stock Toledano.

## Estructura del proyecto

| Ruta | Descripcion |
|---|---|
| `databricks.yml` | Definicion principal del Databricks Asset Bundle, variables y targets por ambiente. |
| `resources/jobs/` | YAML de Databricks Jobs que reemplazan los pipelines ADF. |
| `notebooks/` | Notebooks orquestadores por etapa: control, extraccion, Bronze to Silver, modelo, calidad, publicacion y auditoria. |
| `src/priorizacion_stock_toledano/` | Codigo Python reutilizable para que los notebooks no concentren logica funcional. |
| `sql/` | Scripts SQL de soporte, incluyendo el modelo futuro Lakebase PostgreSQL. |
| `tests/` | Pruebas unitarias con mocks para funciones criticas de control, extraccion, transformacion, modelo, calidad y publicacion. |
| `docs/` | Documentacion tecnica, matriz ADF a Databricks, runbooks y estrategia de evolucion. |

## Componentes principales

### Jobs

Los jobs en `resources/jobs/` definen la orquestacion ejecutable del modelo. El job principal es `job_full_priorizacion_stock`, que coordina auditoria, extracciones, transformaciones, modelo, calidad, reconciliacion, publicacion SQL y notificacion.

### Notebooks

Los notebooks en `notebooks/` actuan como orquestadores de cada etapa. La logica reutilizable se implementa en `src/priorizacion_stock_toledano` para facilitar pruebas, mantenimiento y futuras migraciones.

### src

El paquete `src/priorizacion_stock_toledano/` contiene modulos especializados:

- `control`: lectura de `GetControlCargas` y alternativa Lakebase.
- `extraction`: extraccion SAP HANA y SharePoint.
- `transformations`: transformaciones Bronze to Silver.
- `model`: parametros y calculo del indice.
- `publication`: publicacion JDBC hacia SQL Server.
- `quality`: reglas de calidad y reconciliacion.
- `audit`: auditoria Delta y notificaciones.

### sql

La carpeta `sql/` contiene scripts SQL de apoyo. En `sql/lakebase/` se define la opcion futura para reemplazar gradualmente `GetControlCargas` por Lakebase PostgreSQL.

### tests

La carpeta `tests/` contiene pruebas unitarias para validar contratos, queries, normalizaciones, reglas de calidad, payloads de notificacion y publicacion SQL sin depender de credenciales reales.

## Ambientes

- `dev`
- `test`
- `prod`

## Modos de control

`modo_control` acepta:

- `get_control_cargas`: usa el procedimiento de control externo.
- `lakebase`: usa una tabla/control interno en Databricks Lakebase o Delta.

## Seguridad

El proyecto no contiene credenciales, tokens, URLs firmadas ni secretos embebidos. Todos los valores sensibles deben resolverse por `secret_scope` usando los nombres de secreto declarados como variables del bundle.

## Comandos base

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run priorizacion_stock_toledano -t dev
```
