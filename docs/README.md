# Documentacion

Documentacion tecnica, decisiones de arquitectura, mapas ADF a Databricks y runbooks operativos.

## Indice

| Documento | Descripcion |
|---|---|
| `arquitectura.md` | Vision de arquitectura, capas, componentes y responsabilidades. |
| `estrategia_implementacion.md` | Fases de migracion, criterios de aceptacion y estrategia de cutover. |
| `mapa_adf_priorizacion_stock.md` | Mapa funcional de pipelines, actividades y equivalencias ADF a Databricks. |
| `matriz_adf_databricks.md` | Matriz detallada actividad por actividad con riesgo y observaciones. |
| `operacion.md` | Guia de ejecucion, parametros, auditoria, calidad, publicacion y reprocesos. |
| `pruebas.md` | Estrategia de pruebas, reglas de calidad y validaciones funcionales. |
| `lakebase_evolucion.md` | Diseno de evolucion futura desde `GetControlCargas` hacia Lakebase PostgreSQL. |

## Secciones transversales

La documentacion cubre los componentes principales del repositorio:

- `resources/jobs`: orquestacion declarativa de Databricks Jobs.
- `notebooks`: notebooks orquestadores por capa del pipeline.
- `src`: codigo Python reusable y testeable.
- `sql`: scripts SQL para soporte y evolucion Lakebase.
- `tests`: pruebas unitarias y mocks de contratos criticos.
