# priorizacion-stock-toledano

Databricks Asset Bundle base para migrar y operar el modelo de Priorizacion de Stock Toledano.

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
