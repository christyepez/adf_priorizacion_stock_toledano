-- Normalized Lakebase view consumed by Databricks.

CREATE OR REPLACE VIEW control.vw_control_cargas_priorizacion_stock AS
SELECT
    p.proceso,
    sf.sistema_fuente,
    pf.propietario_fuente,
    cc.columnas_archivo_fuente,
    cc.ruta_archivo_fuente,
    cc.nombre_archivo_fuente,
    cc.filtros_archivo_fuente,
    cc.ruta_archivo_destino,
    cc.nombre_archivo_destino,
    cc.extension_archivo_destino,
    cc.tipo_carga,
    cc.activo,
    cc.orden_ejecucion,
    cc.valid_from,
    cc.valid_to
FROM control.control_cargas cc
INNER JOIN control.proceso_control p
    ON p.proceso_id = cc.proceso_id
INNER JOIN control.sistema_fuente_control sf
    ON sf.sistema_fuente_id = cc.sistema_fuente_id
INNER JOIN control.propietario_fuente_control pf
    ON pf.propietario_fuente_id = cc.propietario_fuente_id
WHERE p.activo = TRUE
  AND sf.activo = TRUE
  AND pf.activo = TRUE;

-- Compatibility view with original ADF/GetControlCargas column names.
CREATE OR REPLACE VIEW control.vw_get_control_cargas_compat AS
SELECT
    proceso AS "Proceso",
    sistema_fuente AS "SistemaFuente",
    propietario_fuente AS "PropietarioFuente",
    columnas_archivo_fuente AS "ColumnasArchivoFuente",
    ruta_archivo_fuente AS "RutaArchivoFuente",
    nombre_archivo_fuente AS "NombreArchivoFuente",
    filtros_archivo_fuente AS "FiltrosArchivoFuente",
    ruta_archivo_destino AS "RutaArchivoDestino",
    nombre_archivo_destino AS "NombreArchivoDestino",
    extension_archivo_destino AS "ExtencionArchivoDestino",
    tipo_carga AS "TipoCarga",
    activo AS "Activo",
    orden_ejecucion AS "OrdenEjecucion",
    valid_from AS "AnioMesDiaInicial",
    valid_to AS "AnioMesDiaFinal"
FROM control.vw_control_cargas_priorizacion_stock;
