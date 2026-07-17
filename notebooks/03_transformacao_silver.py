# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Transformação e integração da camada Silver
# MAGIC
# MAGIC Padroniza nomes, converte tipos, trata valores nulos,
# MAGIC remove duplicidades, normaliza chaves e cria tabelas
# MAGIC Silver com contratos de dados conhecidos.

# COMMAND ----------

import re
import unicodedata
from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_NAME = "alfabetizacao"
NOTEBOOK_NAME = "03_transformacao_silver"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

BRONZE_SCHEMA = f"{PROJECT_NAME}_bronze"
SILVER_SCHEMA = f"{PROJECT_NAME}_silver"
MONITORING_SCHEMA = f"{PROJECT_NAME}_monitoring"

RUN_ID = str(uuid4())
STARTED_AT = datetime.now(timezone.utc)
START_COUNTER = perf_counter()

UF_BY_CODE = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}

UF_NAMES = {
    "RO": "Rondônia",
    "AC": "Acre",
    "AM": "Amazonas",
    "RR": "Roraima",
    "PA": "Pará",
    "AP": "Amapá",
    "TO": "Tocantins",
    "MA": "Maranhão",
    "PI": "Piauí",
    "CE": "Ceará",
    "RN": "Rio Grande do Norte",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "AL": "Alagoas",
    "SE": "Sergipe",
    "BA": "Bahia",
    "MG": "Minas Gerais",
    "ES": "Espírito Santo",
    "RJ": "Rio de Janeiro",
    "SP": "São Paulo",
    "PR": "Paraná",
    "SC": "Santa Catarina",
    "RS": "Rio Grande do Sul",
    "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso",
    "GO": "Goiás",
    "DF": "Distrito Federal",
}

# COMMAND ----------

def snake_case(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode(
        "ascii",
        "ignore",
    ).decode("ascii")

    cleaned = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        ascii_value,
    )
    cleaned = re.sub(r"_+", "_", cleaned)

    return cleaned.strip("_").lower()


def normalize_columns(df: DataFrame) -> DataFrame:
    result = df
    used_names = set()

    for original_name in df.columns:
        normalized_name = snake_case(original_name)
        candidate = normalized_name
        suffix = 2

        while candidate in used_names:
            candidate = f"{normalized_name}_{suffix}"
            suffix += 1

        used_names.add(candidate)

        if original_name != candidate:
            result = result.withColumnRenamed(
                original_name,
                candidate,
            )

    return result


def resolve_column(
    df: DataFrame,
    candidates: list[str],
):
    available = {
        column.lower(): column
        for column in df.columns
    }

    for candidate in candidates:
        normalized_candidate = snake_case(candidate)

        if normalized_candidate in available:
            return available[normalized_candidate]

    return None


def string_expr(
    df: DataFrame,
    candidates: list[str],
    alias: str,
):
    column_name = resolve_column(df, candidates)

    if not column_name:
        return F.lit(None).cast("string").alias(alias)

    cleaned = F.trim(F.col(column_name).cast("string"))

    return (
        F.when(
            F.lower(cleaned).isin(
                "",
                "null",
                "none",
                "nan",
                "na",
                "n/a",
                "não informado",
                "nao informado",
            ),
            F.lit(None),
        )
        .otherwise(cleaned)
        .alias(alias)
    )


def int_expr(
    df: DataFrame,
    candidates: list[str],
    alias: str,
):
    column_name = resolve_column(df, candidates)

    if not column_name:
        return F.lit(None).cast("int").alias(alias)

    return (
        F.regexp_replace(
            F.trim(F.col(column_name).cast("string")),
            r"\.0$",
            "",
        )
        .cast("int")
        .alias(alias)
    )


def double_expr(
    df: DataFrame,
    candidates: list[str],
    alias: str,
):
    column_name = resolve_column(df, candidates)

    if not column_name:
        return F.lit(None).cast("double").alias(alias)

    return (
        F.regexp_replace(
            F.trim(F.col(column_name).cast("string")),
            ",",
            ".",
        )
        .cast("double")
        .alias(alias)
    )


def boolean_expr(
    df: DataFrame,
    candidates: list[str],
    alias: str,
):
    column_name = resolve_column(df, candidates)

    if not column_name:
        return F.lit(None).cast("boolean").alias(alias)

    normalized = F.lower(
        F.trim(F.col(column_name).cast("string"))
    )

    return (
        F.when(
            normalized.isin(
                "1",
                "true",
                "t",
                "sim",
                "s",
                "yes",
                "y",
            ),
            F.lit(True),
        )
        .when(
            normalized.isin(
                "0",
                "false",
                "f",
                "nao",
                "não",
                "n",
                "no",
            ),
            F.lit(False),
        )
        .otherwise(F.lit(None).cast("boolean"))
        .alias(alias)
    )


def uf_from_municipio(column_name: str):
    mapping_items = []

    for code, uf in UF_BY_CODE.items():
        mapping_items.extend([F.lit(code), F.lit(uf)])

    mapping = F.create_map(*mapping_items)

    return F.element_at(
        mapping,
        F.substring(F.col(column_name), 1, 2),
    )


def exact_deduplicate(
    df: DataFrame,
    business_columns: list[str],
) -> DataFrame:
    existing_columns = [
        column
        for column in business_columns
        if column in df.columns
    ]

    hash_columns = [
        F.coalesce(
            F.col(column).cast("string"),
            F.lit("<NULL>"),
        )
        for column in existing_columns
    ]

    return (
        df
        .withColumn(
            "_record_hash",
            F.sha2(
                F.concat_ws("||", *hash_columns),
                256,
            ),
        )
        .dropDuplicates(["_record_hash"])
        .drop("_record_hash")
    )


def add_processing_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "_silver_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_silver_run_id",
            F.lit(RUN_ID),
        )
    )


def write_silver(
    df: DataFrame,
    table_name: str,
) -> int:
    full_table_name = (
        f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    row_count = int(
        spark.table(full_table_name).count()
    )

    print(
        f"{full_table_name}: "
        f"{row_count:,} registros."
    )

    return row_count

# COMMAND ----------

bronze_alunos = normalize_columns(
    spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.alunos"
    )
)

print("Colunas normalizadas da Bronze alunos:")
print(bronze_alunos.columns)

alunos_selected = bronze_alunos.select(
    int_expr(
        bronze_alunos,
        ["ano", "ano_referencia", "nu_ano_avaliacao"],
        "ano",
    ),
    string_expr(
        bronze_alunos,
        ["id_municipio", "codigo_municipio", "co_municipio"],
        "id_municipio",
    ),
    string_expr(
        bronze_alunos,
        ["id_escola", "codigo_escola", "co_escola"],
        "id_escola",
    ),
    string_expr(
        bronze_alunos,
        ["id_aluno", "codigo_aluno", "co_aluno"],
        "id_aluno",
    ),
    string_expr(
        bronze_alunos,
        ["caderno", "co_caderno_lp"],
        "caderno",
    ),
    string_expr(
        bronze_alunos,
        ["serie", "ano_escolar", "tp_serie"],
        "serie",
    ),
    string_expr(
        bronze_alunos,
        ["rede", "dependencia_administrativa", "tp_dependencia"],
        "rede",
    ),
    boolean_expr(
        bronze_alunos,
        ["presenca", "presente", "in_presenca_lp"],
        "presenca",
    ),
    string_expr(
        bronze_alunos,
        ["preenchimento_caderno", "in_preenchimento_lp"],
        "preenchimento_caderno",
    ),
    boolean_expr(
        bronze_alunos,
        ["alfabetizado", "crianca_alfabetizada", "in_alfabetizado"],
        "alfabetizado",
    ),
    double_expr(
        bronze_alunos,
        [
            "proficiencia",
            "proficiencia_lingua_portuguesa",
            "vl_proficiencia_lp",
            "nota",
        ],
        "proficiencia",
    ),
    double_expr(
        bronze_alunos,
        ["peso_aluno", "peso", "vl_peso_aluno_lp"],
        "peso_aluno",
    ),
    string_expr(
        bronze_alunos,
        ["sigla_uf", "uf", "sg_uf"],
        "sigla_uf_origem",
    ),
)

silver_alunos = (
    alunos_selected
    .withColumn(
        "id_municipio",
        F.regexp_replace(
            F.col("id_municipio"),
            r"\.0$",
            "",
        ),
    )
    .withColumn(
        "rede",
        F.upper(F.col("rede")),
    )
    .withColumn(
        "sigla_uf",
        F.coalesce(
            F.upper(F.col("sigla_uf_origem")),
            uf_from_municipio("id_municipio"),
        ),
    )
    .drop("sigla_uf_origem")
    .filter(
        F.col("ano").isNotNull()
        & F.col("id_municipio").isNotNull()
    )
)

silver_alunos = exact_deduplicate(
    silver_alunos,
    [
        "ano",
        "id_municipio",
        "id_escola",
        "id_aluno",
        "serie",
        "rede",
        "alfabetizado",
        "proficiencia",
    ],
)

silver_alunos = add_processing_metadata(
    silver_alunos
)

# COMMAND ----------

bronze_municipio = normalize_columns(
    spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.municipio"
    )
)

municipio_selected = bronze_municipio.select(
    int_expr(
        bronze_municipio,
        ["ano", "ano_referencia"],
        "ano",
    ),
    string_expr(
        bronze_municipio,
        ["id_municipio", "codigo_municipio"],
        "id_municipio",
    ),
    string_expr(
        bronze_municipio,
        ["nome_municipio", "municipio"],
        "nome_municipio",
    ),
    string_expr(
        bronze_municipio,
        ["sigla_uf", "uf"],
        "sigla_uf_origem",
    ),
    string_expr(
        bronze_municipio,
        ["serie", "ano_escolar"],
        "serie",
    ),
    string_expr(
        bronze_municipio,
        ["rede", "dependencia_administrativa"],
        "rede",
    ),
    double_expr(
        bronze_municipio,
        ["taxa_participacao", "percentual_participacao"],
        "taxa_participacao",
    ),
    double_expr(
        bronze_municipio,
        [
            "taxa_alfabetizacao",
            "percentual_alfabetizado",
            "percentual_alfabetizacao",
            "indicador_alfabetizacao",
        ],
        "taxa_alfabetizacao",
    ),
    double_expr(
        bronze_municipio,
        [
            "media_proficiencia",
            "media_portugues",
            "media_lingua_portuguesa",
            "proficiencia_media",
        ],
        "media_proficiencia",
    ),
)

silver_municipio = (
    municipio_selected
    .withColumn(
        "id_municipio",
        F.regexp_replace(
            F.col("id_municipio"),
            r"\.0$",
            "",
        ),
    )
    .withColumn(
        "sigla_uf",
        F.coalesce(
            F.upper(F.col("sigla_uf_origem")),
            uf_from_municipio("id_municipio"),
        ),
    )
    .drop("sigla_uf_origem")
    .withColumn(
        "rede",
        F.upper(F.col("rede")),
    )
    .filter(
        F.col("ano").isNotNull()
        & F.col("id_municipio").isNotNull()
    )
)

silver_municipio = exact_deduplicate(
    silver_municipio,
    [
        "ano",
        "id_municipio",
        "serie",
        "rede",
        "taxa_alfabetizacao",
        "media_proficiencia",
    ],
)

silver_municipio = add_processing_metadata(
    silver_municipio
)

# COMMAND ----------

bronze_uf = normalize_columns(
    spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.uf"
    )
)

uf_selected = bronze_uf.select(
    int_expr(
        bronze_uf,
        ["ano", "ano_referencia"],
        "ano",
    ),
    string_expr(
        bronze_uf,
        ["sigla_uf", "uf"],
        "sigla_uf",
    ),
    string_expr(
        bronze_uf,
        ["serie", "ano_escolar"],
        "serie",
    ),
    string_expr(
        bronze_uf,
        ["rede", "dependencia_administrativa"],
        "rede",
    ),
    double_expr(
        bronze_uf,
        ["taxa_participacao", "percentual_participacao"],
        "taxa_participacao",
    ),
    double_expr(
        bronze_uf,
        [
            "taxa_alfabetizacao",
            "percentual_alfabetizado",
            "percentual_alfabetizacao",
            "indicador_alfabetizacao",
        ],
        "taxa_alfabetizacao",
    ),
    double_expr(
        bronze_uf,
        [
            "media_proficiencia",
            "media_portugues",
            "media_lingua_portuguesa",
            "proficiencia_media",
        ],
        "media_proficiencia",
    ),
)

silver_uf = (
    uf_selected
    .withColumn(
        "sigla_uf",
        F.upper(F.col("sigla_uf")),
    )
    .withColumn(
        "rede",
        F.upper(F.col("rede")),
    )
    .filter(
        F.col("ano").isNotNull()
        & F.col("sigla_uf").isNotNull()
    )
)

silver_uf = exact_deduplicate(
    silver_uf,
    [
        "ano",
        "sigla_uf",
        "serie",
        "rede",
        "taxa_alfabetizacao",
        "media_proficiencia",
    ],
)

silver_uf = add_processing_metadata(silver_uf)

# COMMAND ----------

def build_meta_table(
    source_table: str,
    entity_type: str,
) -> DataFrame:
    raw = normalize_columns(
        spark.table(
            f"{CATALOG}.{BRONZE_SCHEMA}.{source_table}"
        )
    )

    base_expressions = [
        int_expr(
            raw,
            ["ano", "ano_referencia"],
            "ano_referencia",
        ),
        string_expr(
            raw,
            ["rede", "dependencia_administrativa"],
            "rede",
        ),
        double_expr(
            raw,
            [
                "taxa_alfabetizacao",
                "percentual_alfabetizado",
                "percentual_alfabetizacao",
            ],
            "taxa_alfabetizacao_referencia",
        ),
    ]

    if entity_type == "uf":
        base_expressions.append(
            string_expr(
                raw,
                ["sigla_uf", "uf"],
                "sigla_uf",
            )
        )

    if entity_type == "municipio":
        base_expressions.extend(
            [
                string_expr(
                    raw,
                    ["id_municipio", "codigo_municipio"],
                    "id_municipio",
                ),
                string_expr(
                    raw,
                    ["nome_municipio", "municipio"],
                    "nome_municipio",
                ),
                string_expr(
                    raw,
                    ["sigla_uf", "uf"],
                    "sigla_uf_origem",
                ),
                string_expr(
                    raw,
                    [
                        "nivel_alfabetizacao",
                        "nivel",
                        "classificacao",
                    ],
                    "nivel_alfabetizacao",
                ),
            ]
        )

    base_df = raw.select(*base_expressions)

    meta_columns = []

    for column_name in raw.columns:
        match = re.match(
            r"^meta(?:_alfabetizacao)?_(20\d{2})$",
            column_name,
        )

        if match:
            meta_columns.append(
                (column_name, int(match.group(1)))
            )

    if meta_columns:
        meta_items = []

        for column_name, year in sorted(
            meta_columns,
            key=lambda item: item[1],
        ):
            meta_items.append(
                F.struct(
                    F.lit(year).cast("int").alias(
                        "ano_meta"
                    ),
                    F.regexp_replace(
                        F.trim(
                            F.col(column_name).cast("string")
                        ),
                        ",",
                        ".",
                    )
                    .cast("double")
                    .alias("meta_alfabetizacao"),
                )
            )

        base_df = raw.select(
            *base_expressions,
            F.explode(
                F.array(*meta_items)
            ).alias("meta"),
        ).select(
            "*",
            F.col("meta.ano_meta").alias("ano_meta"),
            F.col("meta.meta_alfabetizacao").alias(
                "meta_alfabetizacao"
            ),
        ).drop("meta")

    else:
        fallback_year = resolve_column(
            raw,
            ["ano_meta", "ano", "ano_referencia"],
        )
        fallback_value = resolve_column(
            raw,
            [
                "meta_alfabetizacao",
                "meta",
                "percentual_meta",
            ],
        )

        if not fallback_value:
            raise ValueError(
                f"Nenhuma coluna de meta foi localizada em "
                f"{source_table}. Colunas: {raw.columns}"
            )

        base_df = raw.select(
            *base_expressions,
            (
                F.col(fallback_year).cast("int")
                if fallback_year
                else F.lit(None).cast("int")
            ).alias("ano_meta"),
            F.regexp_replace(
                F.trim(
                    F.col(fallback_value).cast("string")
                ),
                ",",
                ".",
            )
            .cast("double")
            .alias("meta_alfabetizacao"),
        )

    result = (
        base_df
        .withColumn(
            "rede",
            F.upper(F.col("rede")),
        )
        .filter(
            F.col("ano_meta").isNotNull()
            & F.col("meta_alfabetizacao").isNotNull()
        )
        .filter(
            F.col("meta_alfabetizacao").between(
                0.0,
                100.0,
            )
        )
    )

    partition_columns = ["ano_meta", "rede"]

    if entity_type == "uf":
        result = result.withColumn(
            "sigla_uf",
            F.upper(F.col("sigla_uf")),
        )
        partition_columns.append("sigla_uf")

    if entity_type == "municipio":
        result = (
            result
            .withColumn(
                "id_municipio",
                F.regexp_replace(
                    F.col("id_municipio"),
                    r"\.0$",
                    "",
                ),
            )
            .withColumn(
                "sigla_uf",
                F.coalesce(
                    F.upper(F.col("sigla_uf_origem")),
                    uf_from_municipio("id_municipio"),
                ),
            )
            .drop("sigla_uf_origem")
        )
        partition_columns.append("id_municipio")

    window = Window.partitionBy(
        *partition_columns
    ).orderBy(
        F.col("ano_referencia").desc_nulls_last()
    )

    result = (
        result
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    return add_processing_metadata(result)

# COMMAND ----------

silver_meta_brasil = build_meta_table(
    "meta_alfabetizacao_brasil",
    "brasil",
)

silver_meta_uf = build_meta_table(
    "meta_alfabetizacao_uf",
    "uf",
)

silver_meta_municipio = build_meta_table(
    "meta_alfabetizacao_municipio",
    "municipio",
)

# COMMAND ----------

bronze_events = spark.table(
    f"{CATALOG}.{BRONZE_SCHEMA}."
    "eventos_indicador_stream"
)

silver_events = (
    bronze_events
    .select(
        F.trim(F.col("id_evento")).alias("id_evento"),
        F.trim(F.col("id_municipio")).alias(
            "id_municipio"
        ),
        F.col("ano").cast("int").alias("ano"),
        F.upper(F.trim(F.col("rede"))).alias("rede"),
        F.upper(F.trim(F.col("tipo_evento"))).alias(
            "tipo_evento"
        ),
        F.col("valor_indicador").cast("double").alias(
            "valor_indicador"
        ),
        F.col("data_evento").cast("timestamp").alias(
            "data_evento"
        ),
        F.col("lote_origem").cast("string").alias(
            "lote_origem"
        ),
        F.col("_ingestion_timestamp").cast(
            "timestamp"
        ).alias("_ingestion_timestamp"),
    )
    .withColumn(
        "sigla_uf",
        uf_from_municipio("id_municipio"),
    )
    .filter(
        F.col("id_evento").isNotNull()
        & F.col("id_municipio").isNotNull()
        & F.col("ano").isNotNull()
        & F.col("valor_indicador").between(0.0, 100.0)
    )
)

event_window = Window.partitionBy(
    "id_evento"
).orderBy(
    F.col("data_evento").desc_nulls_last(),
    F.col("_ingestion_timestamp").desc_nulls_last(),
)

silver_events = (
    silver_events
    .withColumn(
        "_row_number",
        F.row_number().over(event_window),
    )
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

silver_events = add_processing_metadata(
    silver_events
)

# COMMAND ----------

dim_municipio = (
    silver_municipio
    .select(
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
    )
    .dropDuplicates(["id_municipio"])
)

dim_municipio = add_processing_metadata(
    dim_municipio
)

uf_rows = [
    (code, uf, UF_NAMES[uf])
    for code, uf in UF_BY_CODE.items()
]

dim_uf = spark.createDataFrame(
    uf_rows,
    ["id_uf", "sigla_uf", "nome_uf"],
)

dim_uf = add_processing_metadata(dim_uf)

# COMMAND ----------

outputs = {
    "fato_alunos": silver_alunos,
    "fato_municipio": silver_municipio,
    "fato_uf": silver_uf,
    "meta_alfabetizacao_brasil": silver_meta_brasil,
    "meta_alfabetizacao_uf": silver_meta_uf,
    "meta_alfabetizacao_municipio": (
        silver_meta_municipio
    ),
    "eventos_indicador": silver_events,
    "dim_municipio": dim_municipio,
    "dim_uf": dim_uf,
}

written_rows = 0
table_results = []

for table_name, dataframe in outputs.items():
    row_count = write_silver(
        dataframe,
        table_name,
    )
    written_rows += row_count
    table_results.append(
        (table_name, row_count)
    )

display(
    spark.createDataFrame(
        table_results,
        ["table_name", "row_count"],
    ).orderBy("table_name")
)

# COMMAND ----------

ended_at = datetime.now(timezone.utc)
duration = perf_counter() - START_COUNTER

log_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("notebook", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("start_timestamp", TimestampType(), False),
        StructField("end_timestamp", TimestampType(), False),
        StructField("duration_seconds", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("records_read", LongType(), False),
        StructField("records_written", LongType(), False),
        StructField("message", StringType(), True),
        StructField("created_at", TimestampType(), False),
    ]
)

log_df = spark.createDataFrame(
    [
        (
            RUN_ID,
            NOTEBOOK_NAME,
            "silver",
            STARTED_AT,
            ended_at,
            float(duration),
            "SUCCESS",
            int(written_rows),
            int(written_rows),
            (
                f"{len(outputs)} tabelas Silver "
                "criadas com sucesso."
            ),
            ended_at,
        )
    ],
    schema=log_schema,
)

(
    log_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{MONITORING_SCHEMA}.pipeline_runs"
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{CATALOG}`.`{SILVER_SCHEMA}`"
    )
)

print("Transformação Silver concluída com sucesso.")
