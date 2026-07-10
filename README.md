# priorizacion-stock-toledano

Databricks Asset Bundle base para migrar y operar el modelo de Priorizacion de Stock Toledano.

## Estructura del proyecto

| Ruta | Descripcion |
|---|---|
| `databricks.yml` | Definicion principal del Databricks Asset Bundle, variables y targets por ambiente. |
| `resources/jobs/` | YAML de Databricks Jobs agrupados por flujo operativo. |
| `notebooks/` | Notebooks orquestadores por seccion: control, extraccion, transformacion, modelo, publicacion y operacion. |
| `src/priorizacion_stock_toledano/` | Codigo Python reutilizable para que los notebooks no concentren logica funcional. |
| `sql/` | Scripts SQL de soporte, incluyendo el modelo futuro Lakebase PostgreSQL. |
| `tests/` | Pruebas unitarias con mocks para funciones criticas de control, extraccion, transformacion, modelo, calidad y publicacion. |
| `docs/` | Documentacion tecnica, matriz ADF a Databricks, runbooks y estrategia de evolucion. |

## Componentes principales

### Jobs

Los jobs en `resources/jobs/` definen la orquestacion ejecutable del modelo en cuatro archivos compactos: full, extracciones, transformaciones y modelo/publicacion. El job principal es `job_full_priorizacion_stock`, que coordina auditoria, extracciones, transformaciones, modelo, calidad, reconciliacion, publicacion SQL y notificacion.

### Notebooks

Los notebooks en `notebooks/` actuan como orquestadores de cada etapa. La logica reutilizable se implementa en `src/priorizacion_stock_toledano` para facilitar pruebas, mantenimiento y futuras migraciones.

Cada notebook que importa el paquete del proyecto incluye una celda bootstrap antes de los imports. Esa celda agrega `src/` al `sys.path` cuando el notebook se ejecuta como Workspace File o desde Databricks Asset Bundles, evitando errores `ModuleNotFoundError: priorizacion_stock_toledano` en jobs SAP, SharePoint, calidad, modelo, auditoria y publicacion.

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

La configuracion del bundle queda alineada con el ARM de ADF: `secret_scope` apunta al scope respaldado por Key Vault, SAP HANA y SQL Control usan nombres de secretos, SharePoint recibe una URL base publica sin firmas y OAuth por secretos, y la publicacion SQL usa `sql_publication_server`/`sql_publication_database` como valores no sensibles con usuario/password desde Secret Scope.

## Comandos base

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run job_full_priorizacion_stock -t dev
```
