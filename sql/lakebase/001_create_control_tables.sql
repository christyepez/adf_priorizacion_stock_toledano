-- Lakebase PostgreSQL control model for Priorizacion Stock Toledano.
-- No credentials, endpoints or secrets are stored in this schema.

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.proceso_control (
    proceso_id BIGSERIAL PRIMARY KEY,
    proceso TEXT NOT NULL UNIQUE,
    descripcion TEXT,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS control.sistema_fuente_control (
    sistema_fuente_id BIGSERIAL PRIMARY KEY,
    sistema_fuente TEXT NOT NULL UNIQUE,
    descripcion TEXT,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS control.propietario_fuente_control (
    propietario_fuente_id BIGSERIAL PRIMARY KEY,
    propietario_fuente TEXT NOT NULL UNIQUE,
    descripcion TEXT,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS control.control_cargas (
    control_carga_id BIGSERIAL PRIMARY KEY,
    proceso_id BIGINT NOT NULL REFERENCES control.proceso_control(proceso_id),
    sistema_fuente_id BIGINT NOT NULL REFERENCES control.sistema_fuente_control(sistema_fuente_id),
    propietario_fuente_id BIGINT NOT NULL REFERENCES control.propietario_fuente_control(propietario_fuente_id),
    columnas_archivo_fuente TEXT NOT NULL,
    ruta_archivo_fuente TEXT NOT NULL,
    nombre_archivo_fuente TEXT NOT NULL,
    filtros_archivo_fuente TEXT NOT NULL DEFAULT '',
    ruta_archivo_destino TEXT NOT NULL,
    nombre_archivo_destino TEXT NOT NULL,
    extension_archivo_destino TEXT NOT NULL,
    tipo_carga TEXT NOT NULL DEFAULT '',
    orden_ejecucion INTEGER NOT NULL DEFAULT 0,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from INTEGER NOT NULL DEFAULT 0,
    valid_to INTEGER NOT NULL DEFAULT 99991231,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_control_cargas UNIQUE (
        proceso_id,
        sistema_fuente_id,
        propietario_fuente_id,
        ruta_archivo_fuente,
        nombre_archivo_fuente,
        ruta_archivo_destino,
        nombre_archivo_destino
    )
);

CREATE INDEX IF NOT EXISTS ix_control_cargas_lookup
    ON control.control_cargas (proceso_id, sistema_fuente_id, activo, orden_ejecucion);

CREATE INDEX IF NOT EXISTS ix_control_cargas_owner
    ON control.control_cargas (propietario_fuente_id, activo, orden_ejecucion);
