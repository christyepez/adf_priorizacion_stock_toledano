# Configuracion Databricks y ADF

## 1. Criterio de configuracion

La configuracion operativa del modelo se centraliza en `databricks.yml`.

Los notebooks no deben definir nombres reales de secretos, servidores, storage accounts ni endpoints. Cada job del bundle inyecta los valores necesarios usando variables `${var.*}` definidas en `databricks.yml`.

No se recomienda crear un notebook global de configuracion para que otros notebooks hagan `%run`, porque:

- Introduce acoplamiento entre notebooks.
- Dificulta ejecutar jobs parametrizados por ambiente.
- Duplica la responsabilidad que ya resuelve Databricks Asset Bundles.
- Puede volver a generar errores cuando un notebook se ejecuta aislado.

La alternativa recomendada es:

- `databricks.yml`: fuente de verdad para variables por ambiente.
- `resources/jobs/*.yml`: inyectan variables a cada task.
- `src/priorizacion_stock_toledano`: contiene funciones reutilizables.
- Notebooks: solo orquestan y leen widgets recibidos desde el job.

## 2. Secret Scope

El bundle queda configurado para usar:

```yaml
secret_scope: sc-kv-toledano-bigdata-dev
```

Si el nombre real del Secret Scope cambia, se debe modificar solo esta variable en `databricks.yml`.

El valor `secret-kv-toledano` informado en el inventario corresponde a una key disponible dentro del scope, no al nombre del Secret Scope usado por los notebooks.

## 3. Secretos usados por Priorizacion de Stock

| Componente | Variable bundle | Secret configurado |
|---|---|---|
| ADLS key | `storage_account_key_secret` | `sc-dlsbigdatatoledano-key` |
| ADLS URL opcional | `storage_account_url_secret` | `sc-dlsbigdatatoledano-url` |
| SQL Control server | `sql_control_server_secret` | `sc-sqlbigdatatoledano-server` |
| SQL Control database | `sql_control_database_secret` | `sc-sqlbigdatatoledano-database` |
| SQL Control username | `sql_control_username_secret` | `sc-sqlbigdatatoledano-username` |
| SQL Control password | `sql_control_password_secret` | `sc-sqlbigdatatoledano-password` |
| SAP HANA server | `sap_hana_server_secret` | `sc-saphana-servernode` |
| SAP HANA username | `sap_hana_username_secret` | `sc-saphana-username` |
| SAP HANA password | `sap_hana_password_secret` | `sc-saphana-password` |
| SharePoint client id | `sharepoint_client_id_secret` | `sc-sharepoint-client-id` |
| SharePoint client secret | `sharepoint_client_secret_secret` | `sc-sharepoint-secret-id` |
| SharePoint tenant id | `sharepoint_tenant_id_secret` | `sc-sharepoint-tenant-id` |
| SQL publicacion username | `sql_publication_username_secret` | `sc-sql-orcpanama-username` |
| SQL publicacion password | `sql_publication_password_secret` | `sc-sql-orcpanama-password` |
| ADF Databricks workspace | `adf_databricks_workspace_secret` | `sc-adbbigdatatoledano-workspace` |
| ADF Databricks resource id | `adf_databricks_workspace_resource_id_secret` | `sc-adbbigdatatoledano-workspaces-resourceid` |
| ADF Databricks token | `adf_databricks_token_secret` | `sc-adbbigdatatoledano-token` |
| ADF Databricks cluster | `adf_databricks_cluster_secret` | `sc-adbbigdatatoledano-cluster` |

## 4. Secretos no usados por este modelo

Estos secretos existen en Databricks, pero no son requeridos por Priorizacion de Stock Toledano:

- `sc-conn-arcgis-mercadeo-pass`
- `sc-conn-arcgis-mercadeo-user`
- `sc-dichterneira-password`
- `sc-dichterneira-username`
- `sc-gruporey-password`
- `sc-gruporey-username`
- `sc-sqlbigdatatoledano-pass`
- `sc-sql-mercadeo-pass`
- `sc-sql-mercadeo-user`
- `sc-sqlbigdatapronaca-database`
- `sc-sqlbigdatapronaca-password`
- `sc-sqlbigdatapronaca-server`
- `sc-sqlbigdatapronaca-username`
- `secret-kv-toledano`

## 5. Notificaciones

La lista de secretos informada no contiene una key de endpoint Logic App. Por eso:

```yaml
notification_endpoint_secret: ""
notification_enabled: "false"
```

Cuando exista una key real para el endpoint de notificacion, configurar:

```yaml
notification_endpoint_secret: <nombre_del_secreto_endpoint>
notification_enabled: "true"
```

## 6. Paso a paso para configurar Databricks desde ADF

### 6.1 Crear o validar Secret Scope en Databricks

1. Abrir Databricks.
2. Validar que exista el Secret Scope `sc-kv-toledano-bigdata-dev`.
3. Confirmar que dentro del scope existan las keys usadas por el modelo.
4. No guardar valores en notebooks ni YAML; solo nombres de keys.

Validacion recomendada en un notebook temporal de Databricks:

```python
scope = "sc-kv-toledano-bigdata-dev"
required_keys = [
    "sc-sqlbigdatatoledano-server",
    "sc-sqlbigdatatoledano-database",
    "sc-sqlbigdatatoledano-username",
    "sc-sqlbigdatatoledano-password",
    "sc-saphana-servernode",
    "sc-saphana-username",
    "sc-saphana-password",
    "sc-sharepoint-client-id",
    "sc-sharepoint-secret-id",
    "sc-sharepoint-tenant-id",
    "sc-sql-orcpanama-username",
    "sc-sql-orcpanama-password",
]

for key in required_keys:
    dbutils.secrets.get(scope, key)
    print(f"{key}: OK")
```

Si una key aparece en el inventario pero `dbutils.secrets.get` falla, revisar permisos `READ` del scope para el usuario o service principal que ejecuta el job.

### 6.2 Configurar Linked Service de Key Vault en ADF

1. En ADF, crear o validar `LS_Key_Vault`.
2. Configurar la URL del Key Vault.
3. Validar que ADF pueda leer secretos desde ese Key Vault.
4. Usar Managed Identity cuando aplique.

### 6.3 Configurar Linked Service Databricks en ADF

1. Crear o validar un Linked Service Azure Databricks.
2. Resolver workspace con `sc-adbbigdatatoledano-workspace` o `sc-adbbigdatatoledano-workspaces-resourceid`.
3. Resolver token con `sc-adbbigdatatoledano-token` si no se usa MSI.
4. Resolver cluster con `sc-adbbigdatatoledano-cluster` si ADF ejecuta notebooks en existing cluster.
5. No hardcodear host, token ni cluster id en ADF.

### 6.4 Desplegar el bundle

Desde una maquina con Databricks CLI configurado:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

### 6.5 Ejecutar desde Databricks Jobs

Ejecutar:

```bash
databricks bundle run job_full_priorizacion_stock -t dev
```

Tambien se pueden ejecutar jobs parciales:

```bash
databricks bundle run job_ext_saphana_priorizacion_stock -t dev
databricks bundle run job_ext_sharepoint_priorizacion_stock -t dev
```

### 6.6 Orquestar desde ADF

Para convivencia con ADF hay dos opciones:

1. ADF llama notebooks Databricks directamente usando el Linked Service.
2. ADF dispara Databricks Jobs ya desplegados por el bundle.

La opcion recomendada es la segunda, porque el bundle controla dependencias, parametros, retries, auditoria y versionado.

### 6.7 Validaciones iniciales

1. Ejecutar primero `job_ext_saphana_priorizacion_stock`.
2. Validar que `GetControlCargas` devuelve registros activos.
3. Ejecutar `job_ext_sharepoint_priorizacion_stock`.
4. Validar escritura en Bronze.
5. Ejecutar `job_full_priorizacion_stock` cuando las extracciones funcionen.

## 7. Variables que pueden requerir ajuste manual

Si el Secret Scope contiene las keys SQL Control, dejar:

```yaml
sql_control_server: ""
sql_control_database: ""
```

Si esas dos keys no estan disponibles pero servidor/base no son sensibles, configurar:

```yaml
sql_control_server: "<servidor_sql_control>"
sql_control_database: "<base_sql_control>"
```

Usuario y password deben permanecer siempre en Secret Scope.

## 8. Lectura de GetControlCargas

El bundle soporta dos modos:

```yaml
sql_control_read_mode: jdbc_sp
```

Usa SQL Server externo y ejecuta `conf.GetControlCargas` por JDBC directo. Es el modo compatible con ADF y con el procedimiento original.

```yaml
sql_control_read_mode: spark_sql
```

Usa `spark.sql(...)` cuando el control ya fue expuesto como vista, tabla o query dentro de Databricks. En este modo se debe configurar al menos una de estas variables:

```yaml
sql_control_spark_relation: conf.vw_control_cargas_priorizacion_stock
```

o una query completa:

```yaml
sql_control_spark_sql: >
  SELECT *
  FROM conf.vw_control_cargas_priorizacion_stock
  WHERE Proceso = 'Modelo_Priorizacion_Stock'
    AND SistemaFuente = 'SapHana'
```

No usar `spark.sql("EXEC conf.GetControlCargas ...")` salvo que ese `EXEC` exista realmente en el motor SQL de Databricks. Para SQL Server remoto se debe mantener `jdbc_sp`.
