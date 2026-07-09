-- Sample configuration for Proceso = Modelo_Priorizacion_Stock.
-- Values are examples for Lakebase adoption and must be aligned with productive control data before cutover.

INSERT INTO control.proceso_control (proceso, descripcion, activo)
VALUES
    ('Modelo_Priorizacion_Stock', 'Modelo de priorizacion de stock Toledano', TRUE)
ON CONFLICT (proceso) DO UPDATE
SET descripcion = EXCLUDED.descripcion,
    activo = EXCLUDED.activo,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO control.sistema_fuente_control (sistema_fuente, descripcion, activo)
VALUES
    ('SapHana', 'Vistas SAP HANA consumidas por el modelo', TRUE),
    ('sharepoint', 'Archivos del portal de informacion SharePoint', TRUE)
ON CONFLICT (sistema_fuente) DO UPDATE
SET descripcion = EXCLUDED.descripcion,
    activo = EXCLUDED.activo,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO control.propietario_fuente_control (propietario_fuente, descripcion, activo)
VALUES
    ('VistasSapHana', 'Objetos origen publicados como vistas SAP HANA', TRUE),
    ('DatosPortalDeInformacion', 'Archivos compartidos en el portal de informacion', TRUE)
ON CONFLICT (propietario_fuente) DO UPDATE
SET descripcion = EXCLUDED.descripcion,
    activo = EXCLUDED.activo,
    updated_at = CURRENT_TIMESTAMP;

WITH refs AS (
    SELECT
        p.proceso_id,
        sf.sistema_fuente_id,
        pf.propietario_fuente_id,
        sf.sistema_fuente,
        pf.propietario_fuente
    FROM control.proceso_control p
    CROSS JOIN control.sistema_fuente_control sf
    CROSS JOIN control.propietario_fuente_control pf
    WHERE p.proceso = 'Modelo_Priorizacion_Stock'
)
INSERT INTO control.control_cargas (
    proceso_id,
    sistema_fuente_id,
    propietario_fuente_id,
    columnas_archivo_fuente,
    ruta_archivo_fuente,
    nombre_archivo_fuente,
    filtros_archivo_fuente,
    ruta_archivo_destino,
    nombre_archivo_destino,
    extension_archivo_destino,
    tipo_carga,
    orden_ejecucion,
    activo
)
SELECT
    proceso_id,
    sistema_fuente_id,
    propietario_fuente_id,
    columnas_archivo_fuente,
    ruta_archivo_fuente,
    nombre_archivo_fuente,
    filtros_archivo_fuente,
    ruta_archivo_destino,
    nombre_archivo_destino,
    extension_archivo_destino,
    tipo_carga,
    orden_ejecucion,
    activo
FROM (
    SELECT
        refs.proceso_id,
        refs.sistema_fuente_id,
        refs.propietario_fuente_id,
        '*' AS columnas_archivo_fuente,
        'SAP_SCHEMA' AS ruta_archivo_fuente,
        'CV_M_OFERTAS_DOC_VENTAS' AS nombre_archivo_fuente,
        '' AS filtros_archivo_fuente,
        'sap' AS ruta_archivo_destino,
        'fact_cv_m_ofertas_doc_ventas' AS nombre_archivo_destino,
        '.parquet' AS extension_archivo_destino,
        'full' AS tipo_carga,
        10 AS orden_ejecucion,
        TRUE AS activo
    FROM refs
    WHERE refs.sistema_fuente = 'SapHana'
      AND refs.propietario_fuente = 'VistasSapHana'

    UNION ALL

    SELECT
        refs.proceso_id,
        refs.sistema_fuente_id,
        refs.propietario_fuente_id,
        '*' AS columnas_archivo_fuente,
        'SAP_SCHEMA' AS ruta_archivo_fuente,
        'CV_LO_PEDIDO' AS nombre_archivo_fuente,
        '' AS filtros_archivo_fuente,
        'sap' AS ruta_archivo_destino,
        'fact_cv_lo_pedido' AS nombre_archivo_destino,
        '.parquet' AS extension_archivo_destino,
        'full' AS tipo_carga,
        20 AS orden_ejecucion,
        TRUE AS activo
    FROM refs
    WHERE refs.sistema_fuente = 'SapHana'
      AND refs.propietario_fuente = 'VistasSapHana'

    UNION ALL

    SELECT
        refs.proceso_id,
        refs.sistema_fuente_id,
        refs.propietario_fuente_id,
        '*' AS columnas_archivo_fuente,
        '/sites/portal/DatosPortalDeInformacion' AS ruta_archivo_fuente,
        'grupos_priorizacion' AS nombre_archivo_fuente,
        '' AS filtros_archivo_fuente,
        'sharepoint/datos_portal_de_informacion/grupos_priorizacion' AS ruta_archivo_destino,
        'grupos_priorizacion' AS nombre_archivo_destino,
        '.xlsx' AS extension_archivo_destino,
        'file' AS tipo_carga,
        10 AS orden_ejecucion,
        TRUE AS activo
    FROM refs
    WHERE refs.sistema_fuente = 'sharepoint'
      AND refs.propietario_fuente = 'DatosPortalDeInformacion'

    UNION ALL

    SELECT
        refs.proceso_id,
        refs.sistema_fuente_id,
        refs.propietario_fuente_id,
        '*' AS columnas_archivo_fuente,
        '/sites/portal/DatosPortalDeInformacion' AS ruta_archivo_fuente,
        'priorizaciones_previas' AS nombre_archivo_fuente,
        '' AS filtros_archivo_fuente,
        'sharepoint/datos_portal_de_informacion/priorizaciones_previas' AS ruta_archivo_destino,
        'priorizaciones_previas' AS nombre_archivo_destino,
        '.xlsx' AS extension_archivo_destino,
        'file' AS tipo_carga,
        20 AS orden_ejecucion,
        TRUE AS activo
    FROM refs
    WHERE refs.sistema_fuente = 'sharepoint'
      AND refs.propietario_fuente = 'DatosPortalDeInformacion'
) sample_rows
ON CONFLICT ON CONSTRAINT uq_control_cargas DO UPDATE
SET columnas_archivo_fuente = EXCLUDED.columnas_archivo_fuente,
    filtros_archivo_fuente = EXCLUDED.filtros_archivo_fuente,
    extension_archivo_destino = EXCLUDED.extension_archivo_destino,
    tipo_carga = EXCLUDED.tipo_carga,
    orden_ejecucion = EXCLUDED.orden_ejecucion,
    activo = EXCLUDED.activo,
    updated_at = CURRENT_TIMESTAMP;
