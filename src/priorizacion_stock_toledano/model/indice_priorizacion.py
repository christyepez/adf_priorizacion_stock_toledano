from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_NUM_MESES = 12
DEFAULT_COD_CENTROS = ["3900", "3903", "3905", "3904"]
EARTH_RADIUS_KM = 6371.0

CENTROS_DISTRIBUCION = [
    (3900, 9.0747526, -79.4278550),
    (3903, 8.4394907, -82.4650681),
    (3905, 8.1231047, -80.6981549),
    (3904, 9.4686099, -82.5180536),
]

REQUIRED_TABLE_KEYS = [
    "TBL_RENTABILIDAD_SKU",
    "TBL_COSTO_SERVIR",
    "TBL_DIM_MATERIALES",
    "TBL_DIM_CLIENTES",
    "TBL_CLIENTES_PRIORIZADOS",
    "TBL_INDICE_PRIORIZACION_ANTERIOR",
    "TBL_OUTPUT_INDICE_PRIORIZACION",
    "TBL_OUTPUT_INDICE_PRIORIZACION_HISTORICO",
]

REQUIRED_COLUMNS = {
    "RENTABILIDAD": ["fecha", "cod_cliente", "cc_utilidad_bruta", "valor_neto"],
    "COSTO_SERVIR": ["fecha"],
    "DIM_CLIENTES": [
        "cod_cliente",
        "cod_cadena",
        "cod_canal1",
        "cod_estructura",
        "cod_sub_canal",
        "cod_tipologia",
        "nombre_cliente",
        "grupo_economico",
        "desc_cadena",
        "desc_canal",
        "canal_atencion",
        "longitud",
        "latitud",
        "id_centro",
        "lprio",
    ],
}


@dataclass(frozen=True)
class ModelRunMetrics:
    input_counts: dict[str, int]
    output_count: int
    output_table: str
    output_history_table: str


def validate_input_tables(tablas_modelo: dict[str, str]) -> None:
    missing = [key for key in REQUIRED_TABLE_KEYS if not tablas_modelo.get(key)]
    if missing:
        raise ValueError(f"Faltan tablas requeridas del modelo: {', '.join(missing)}")


def require_columns(df: Any, required_columns: list[str], table_name: str) -> None:
    current = set(getattr(df, "columns", []))
    missing = [column for column in required_columns if column not in current]
    if missing:
        raise ValueError(f"{table_name} no contiene columnas requeridas: {', '.join(missing)}")


def metric_columns_present(
    columns: list[str],
    *,
    col_margen: str = "margen_bruto_sum",
    col_estabilidad: str = "cv_margen_bruto",
    col_distancia: str = "distancia",
    col_margen_prom: str = "margen_bruto_prom",
) -> dict[str, bool]:
    available = set(columns)
    return {
        "margen": col_margen in available,
        "estabilidad": col_estabilidad in available,
        "distancia": col_distancia in available,
        "margen_prom": col_margen_prom in available,
    }


def _sum_column(column_name: str, alias_name: str) -> Any:
    from pyspark.sql.functions import col, sum as _sum

    return _sum(col(column_name)).alias(alias_name)


def calcular_margen_acumulado(df: Any, col_id: str, col_margen: str, alias_name: str) -> Any:
    return df.groupBy(col_id).agg(_sum_column(col_margen, alias_name))


def crear_indice_priorizacion(
    df: Any,
    col_margen: str = "margen_bruto_sum",
    col_estabilidad: str = "cv_margen_bruto",
    col_distancia: str = "distancia",
    col_margen_prom: str = "margen_bruto_prom",
) -> Any:
    from pyspark.sql.functions import col, lit, max as _max, min as _min

    presence = metric_columns_present(
        df.columns,
        col_margen=col_margen,
        col_estabilidad=col_estabilidad,
        col_distancia=col_distancia,
        col_margen_prom=col_margen_prom,
    )

    columnas_stats = []
    if presence["margen"]:
        columnas_stats.extend([_min(col_margen).alias("min_margen"), _max(col_margen).alias("max_margen")])
    if presence["estabilidad"]:
        columnas_stats.extend([_min(col_estabilidad).alias("min_est"), _max(col_estabilidad).alias("max_est")])
    if presence["distancia"]:
        columnas_stats.extend([_min(col_distancia).alias("min_dist"), _max(col_distancia).alias("max_dist")])
    if presence["margen_prom"]:
        columnas_stats.extend(
            [_min(col_margen_prom).alias("min_margen_prom"), _max(col_margen_prom).alias("max_margen_prom")]
        )

    if not columnas_stats:
        return df

    stats = df.select(*columnas_stats).collect()[0]

    if presence["distancia"]:
        min_d = stats["min_dist"]
        max_d = stats["max_dist"]
        rango_d = max(max_d - min_d, 1e-9)
        df = df.withColumn("distancia_norm", 1 - ((col(col_distancia) - lit(min_d)) / lit(rango_d)))

    if presence["margen"]:
        min_m = stats["min_margen"]
        max_m = stats["max_margen"]
        rango_m = max(max_m - min_m, 1e-9)
        df = df.withColumn("margen_norm", (col(col_margen) - lit(min_m)) / lit(rango_m))

    if presence["estabilidad"]:
        min_e = stats["min_est"]
        max_e = stats["max_est"]
        rango_e = max(max_e - min_e, 1e-9)
        df = df.withColumn("estabilidad_norm", 1 - ((col(col_estabilidad) - lit(min_e)) / lit(rango_e)))

    if presence["margen_prom"]:
        min_mp = stats["min_margen_prom"]
        max_mp = stats["max_margen_prom"]
        rango_mp = max(max_mp - min_mp, 1e-9)
        df = df.withColumn("margen_bruto_prom_norm", (col(col_margen_prom) - lit(min_mp)) / lit(rango_mp))

    return df


def crear_score_priorizacion(
    df: Any,
    peso_margen: float,
    peso_estabilidad: float,
    peso_distancia: float,
    col_margen: str = "margen_norm",
    col_estabilidad: str = "estabilidad_norm",
    col_distancia: str = "distancia_norm",
    nombre_score: str = "score_margen_bruto",
    nombre_rank: str = "indice",
) -> Any:
    from pyspark.sql.functions import col, desc, lit, min as _min, row_number, when
    from pyspark.sql.window import Window

    df = df.withColumn(
        nombre_score,
        when(
            col(col_margen).isNotNull() & col(col_estabilidad).isNotNull() & col(col_distancia).isNotNull(),
            peso_margen * col(col_margen) + peso_estabilidad * col(col_estabilidad) + peso_distancia * col(col_distancia),
        ),
    )

    min_score = df.agg(_min(col(nombre_score)).alias("min_score")).collect()[0]["min_score"]
    df = df.withColumn(nombre_score, when(col(nombre_score).isNull(), lit(min_score)).otherwise(col(nombre_score)))

    window_spec = Window.orderBy(desc(nombre_score))
    return df.withColumn(nombre_rank, row_number().over(window_spec))


def calcular_margen_porcentual_promedio_mensual(
    df: Any,
    col_id: str = "cod_cliente_homologado",
    col_fecha: str = "fecha",
    col_margen: str = "margen_bruto",
    col_venta: str = "venta_neta",
    alias_name: str = "margen_porcentual_prom",
) -> Any:
    from pyspark.sql.functions import avg, col, sum as _sum, when

    df_mensual = (
        df.groupBy(col_id, col_fecha)
        .agg(
            _sum(col(col_margen)).alias("margen_bruto_mes"),
            _sum(col(col_venta)).alias("venta_neta_mes"),
        )
        .withColumn(
            "margen_porcentual_mes",
            when(
                (col("venta_neta_mes") != 0)
                & ~((col("margen_bruto_mes") < 0) & (col("venta_neta_mes") < 0)),
                col("margen_bruto_mes") / col("venta_neta_mes"),
            ),
        )
    )

    return df_mensual.groupBy(col_id).agg(avg(col("margen_porcentual_mes")).alias(alias_name))


def crear_dataframe_centros(spark: Any) -> Any:
    from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

    schema = StructType(
        [
            StructField("id_centro", IntegerType(), False),
            StructField("latitud_centro", DoubleType(), False),
            StructField("longitud_centro", DoubleType(), False),
        ]
    )
    return spark.createDataFrame(CENTROS_DISTRIBUCION, schema)


def resolver_fecha_filtro(df_costo_servir: Any, fecha_filtro: str | None) -> Any:
    from pyspark.sql.functions import col, lit, max as _max, to_date

    if not fecha_filtro:
        return df_costo_servir.agg(_max(col("fecha")).alias("max_anio_mes")).collect()[0]["max_anio_mes"]
    return to_date(lit(fecha_filtro))


def calcular_indice_priorizacion(
    spark: Any,
    tablas_modelo: dict[str, str],
    *,
    ambiente: str,
    execution_id: str,
    fecha_filtro: str | None = None,
    num_meses: int = DEFAULT_NUM_MESES,
    cod_centros: list[str] | None = None,
    aplicar_union: str = "no",
) -> tuple[Any, ModelRunMetrics]:
    from pyspark.sql.functions import (
        add_months,
        asin,
        col,
        cos,
        current_timestamp,
        from_utc_timestamp,
        lit,
        max as _max,
        radians,
        regexp_replace,
        row_number,
        sin,
        sqrt,
        sum as _sum,
        to_date,
        trim,
        when,
    )
    from pyspark.sql.window import Window

    validate_input_tables(tablas_modelo)
    centros = cod_centros or DEFAULT_COD_CENTROS

    rentabilidad_table = tablas_modelo["TBL_RENTABILIDAD_SKU"]
    costo_servir_table = tablas_modelo["TBL_COSTO_SERVIR"]
    dim_materiales_table = tablas_modelo["TBL_DIM_MATERIALES"]
    dim_clientes_table = tablas_modelo["TBL_DIM_CLIENTES"]
    clientes_priorizados_table = tablas_modelo["TBL_CLIENTES_PRIORIZADOS"]
    indice_anterior_table = tablas_modelo["TBL_INDICE_PRIORIZACION_ANTERIOR"]
    output_table = tablas_modelo["TBL_OUTPUT_INDICE_PRIORIZACION"]
    output_hist_table = tablas_modelo["TBL_OUTPUT_INDICE_PRIORIZACION_HISTORICO"]

    df_rentabilidad = spark.table(rentabilidad_table)
    df_costo_servir = spark.table(costo_servir_table)
    df_dim_materiales = spark.table(dim_materiales_table)
    df_dim_clientes = spark.table(dim_clientes_table)
    df_priorizados = spark.table(clientes_priorizados_table)
    df_indice_anterior_control = spark.table(indice_anterior_table)

    require_columns(df_rentabilidad, REQUIRED_COLUMNS["RENTABILIDAD"], "RENTABILIDAD")
    require_columns(df_costo_servir, REQUIRED_COLUMNS["COSTO_SERVIR"], "COSTO_SERVIR")
    require_columns(df_dim_clientes, REQUIRED_COLUMNS["DIM_CLIENTES"], "DIM_CLIENTES")

    input_counts = {
        "RENTABILIDAD": df_rentabilidad.count(),
        "COSTO_SERVIR": df_costo_servir.count(),
        "DIM_MATERIALES": df_dim_materiales.count(),
        "DIM_CLIENTES": df_dim_clientes.count(),
        "CLIENTES_PRIORIZADOS": df_priorizados.count(),
        "INDICE_ANTERIOR": df_indice_anterior_control.count(),
    }

    fecha_corte = resolver_fecha_filtro(df_costo_servir, fecha_filtro)
    df_centros = crear_dataframe_centros(spark)

    df_indice_ant = (
        df_dim_clientes.select("cod_cliente", col("lprio").alias("priorizacion"))
        .distinct()
        .filter(col("priorizacion").isNotNull())
    )

    df_dim_clientes = df_dim_clientes.filter(col("id_centro").isin(centros))

    df_cadena = df_dim_clientes.select("cod_cliente", "cod_cadena", "grupo_economico").distinct()
    df_cliente = df_dim_clientes.select(
        "cod_cliente",
        "cod_canal1",
        "cod_estructura",
        "cod_sub_canal",
        "cod_tipologia",
        "nombre_cliente",
        "grupo_economico",
        "desc_cadena",
        "desc_canal",
        "canal_atencion",
        "longitud",
        "latitud",
        "id_centro",
    ).distinct()

    df_cliente = df_cliente.withColumn("latitud", trim(col("latitud"))).withColumn("longitud", trim(col("longitud")))

    df_rentabilidad = df_rentabilidad.filter(
        to_date(col("fecha")).between(add_months(lit(fecha_corte), -num_meses + 1), lit(fecha_corte))
    )

    df_rentabilidad = (
        df_rentabilidad.groupBy("cod_cliente", "fecha")
        .agg(
            _sum(col("cc_utilidad_bruta")).alias("margen_bruto"),
            _sum(col("valor_neto")).alias("venta_neta"),
        )
        .join(df_cadena, "cod_cliente", "left")
        .withColumn(
            "cod_cliente_homologado",
            when(trim(col("cod_cadena")) != "", col("cod_cadena")).otherwise(col("cod_cliente")),
        )
    )

    df_catalogo_clientes = (
        df_rentabilidad.select("cod_cliente", "cod_cadena")
        .distinct()
        .withColumn(
            "cod_cliente_homologado",
            when(trim(col("cod_cadena")) != "", col("cod_cadena")).otherwise(col("cod_cliente")),
        )
    )

    df_cliente_geo = (
        df_cliente.withColumn("latitud", regexp_replace(col("latitud"), r"[^\d.\-]", "").cast("double"))
        .withColumn("longitud", regexp_replace(col("longitud"), r"[^\d.\-]", "").cast("double"))
        .join(df_centros, "id_centro", "left")
    )

    cond_ok = col("latitud").isNotNull() & col("longitud").isNotNull()

    df_distancias = (
        df_rentabilidad.select("cod_cliente", "cod_cliente_homologado")
        .distinct()
        .join(df_cliente_geo.select("cod_cliente", "latitud", "longitud", "latitud_centro", "longitud_centro"), "cod_cliente", "left")
        .withColumn(
            "distancia",
            when(
                cond_ok,
                lit(2 * EARTH_RADIUS_KM)
                * asin(
                    sqrt(
                        sin((radians(col("latitud_centro")) - radians(col("latitud"))) / 2) ** 2
                        + cos(radians(col("latitud")))
                        * cos(radians(col("latitud_centro")))
                        * sin((radians(col("longitud_centro")) - radians(col("longitud"))) / 2) ** 2
                    )
                ),
            ),
        )
    )

    df_distancia_homologado = df_distancias.groupBy("cod_cliente_homologado").agg(_max("distancia").alias("distancia"))
    df_distancia_cliente = df_distancias.groupBy("cod_cliente").agg(_max("distancia").alias("distancia_cliente"))

    df_rentabilidad_homologada = df_rentabilidad.groupBy("cod_cliente_homologado", "fecha").agg(
        _sum(col("margen_bruto")).alias("margen_bruto"),
        _sum(col("venta_neta")).alias("venta_neta"),
    )

    df_rentabilidad_sum = calcular_margen_acumulado(
        df_rentabilidad_homologada, "cod_cliente_homologado", "margen_bruto", "margen_bruto_sum"
    )
    df_rentabilidad_cliente_sum = calcular_margen_acumulado(
        df_rentabilidad, "cod_cliente", "margen_bruto", "margen_bruto_cliente_sum"
    )
    df_venta_sum = calcular_margen_acumulado(df_rentabilidad_homologada, "cod_cliente_homologado", "venta_neta", "venta_neta_sum")
    df_venta_cliente_sum = calcular_margen_acumulado(df_rentabilidad, "cod_cliente", "venta_neta", "venta_neta_cliente_sum")

    df_margen_pct_prom = calcular_margen_porcentual_promedio_mensual(
        df_rentabilidad_homologada,
        col_id="cod_cliente_homologado",
        alias_name="margen_porcentual_prom",
    )
    df_margen_pct_prom_cliente = calcular_margen_porcentual_promedio_mensual(
        df_rentabilidad,
        col_id="cod_cliente",
        alias_name="margen_porcentual_prom_cliente",
    )

    df_rentabilidad_fin = (
        df_rentabilidad_sum.join(df_venta_sum, "cod_cliente_homologado", "left")
        .join(df_margen_pct_prom, "cod_cliente_homologado", "left")
        .join(df_distancia_homologado, "cod_cliente_homologado", "left")
        .withColumn(
            "margen_porcentual",
            when(
                (col("venta_neta_sum") != 0)
                & ~((col("margen_bruto_sum") < 0) & (col("venta_neta_sum") < 0)),
                col("margen_bruto_sum") / col("venta_neta_sum"),
            ),
        )
        .filter(col("margen_bruto_sum").isNotNull())
    )
    df_rentabilidad_fin = crear_indice_priorizacion(df_rentabilidad_fin)

    df_rentabilidad_cliente_fin = (
        df_rentabilidad_cliente_sum.join(df_venta_cliente_sum, "cod_cliente", "left")
        .join(df_margen_pct_prom_cliente, "cod_cliente", "left")
        .join(df_distancia_cliente, "cod_cliente", "left")
        .withColumn(
            "margen_porcentual",
            when(
                (col("venta_neta_cliente_sum") != 0)
                & ~((col("margen_bruto_cliente_sum") < 0) & (col("venta_neta_cliente_sum") < 0)),
                col("margen_bruto_cliente_sum") / col("venta_neta_cliente_sum"),
            ),
        )
    )
    df_rentabilidad_cliente_fin = crear_indice_priorizacion(
        df_rentabilidad_cliente_fin,
        col_margen="margen_bruto_cliente_sum",
        col_estabilidad="cv_margen_bruto_cliente",
        col_distancia="distancia_cliente",
        col_margen_prom="margen_bruto_prom_cliente",
    )

    df_rentabilidad_fin = crear_score_priorizacion(
        df_rentabilidad_fin,
        peso_margen=0.33,
        peso_estabilidad=0.55,
        peso_distancia=0.17,
        col_margen="margen_porcentual",
        col_estabilidad="margen_porcentual_prom",
        nombre_score="score_margen_porcentual",
        nombre_rank="indice_margen_porcentual",
    )
    df_rentabilidad_cliente_fin = crear_score_priorizacion(
        df_rentabilidad_cliente_fin,
        peso_margen=0.33,
        peso_estabilidad=0.5,
        peso_distancia=0.17,
        col_margen="margen_porcentual",
        col_estabilidad="margen_porcentual_prom_cliente",
        nombre_score="score_margen_porcentual_cliente",
        nombre_rank="indice_margen_porcentual_cliente",
    )

    df_rentabilidad_cliente_fin = df_rentabilidad_cliente_fin.select("cod_cliente", "indice_margen_porcentual_cliente")

    df_indice = df_rentabilidad_fin.join(df_catalogo_clientes, "cod_cliente_homologado", "left")
    df_indice = df_indice.join(df_cliente, "cod_cliente", "left")
    df_indice = df_indice.join(df_rentabilidad_cliente_fin, "cod_cliente", "left")
    df_indice = df_indice.join(df_indice_ant, "cod_cliente", "left")

    if aplicar_union == "si" and "orden_grupo" in df_indice.columns:
        df_indice = df_indice.withColumn(
            "priorizacion",
            when(col("priorizacion").isNull(), col("orden_grupo")).otherwise(col("priorizacion")),
        )

    window_final = Window.orderBy(
        col("priorizacion").asc_nulls_last(),
        col("indice_margen_porcentual").asc_nulls_last(),
        col("indice_margen_porcentual_cliente").asc_nulls_last(),
    )

    df_indice = df_indice.withColumn("indice_margen_porcentual_fin", row_number().over(window_final))
    df_indice = df_indice.select(
        "cod_cliente",
        col("indice_margen_porcentual_fin").alias("prioridad"),
        "margen_porcentual",
    )

    df_indice = (
        df_indice.withColumn("Estatus", lit("N"))
        .withColumn("execution_id", lit(execution_id))
        .withColumn("create_timestamp", from_utc_timestamp(current_timestamp(), "America/Bogota"))
    )

    output_count = df_indice.count()
    metrics = ModelRunMetrics(
        input_counts=input_counts,
        output_count=output_count,
        output_table=output_table,
        output_history_table=output_hist_table,
    )
    return df_indice, metrics


def escribir_salidas_gold(df_indice: Any, output_table: str, output_hist_table: str) -> None:
    (
        df_indice.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(output_table)
    )
    (
        df_indice.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(output_hist_table)
    )
