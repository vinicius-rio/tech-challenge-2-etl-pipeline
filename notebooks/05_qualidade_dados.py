# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Qualidade de dados
# MAGIC
# MAGIC Executa regras de completude, unicidade, domínio e integridade
# MAGIC referencial nas camadas Bronze, Silver e Gold.

# COMMAND ----------

from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from pyspark.sql import DataFrame
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
NOTEBOOK_NAME = "05_qualidade_dados"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

BRONZE_SCHEMA = f"{PROJECT_NAME}_bronze"
SILVER_SCHEMA = f"{PROJECT_NAME}_silver"
GOLD_SCHEMA = f"{PROJECT_NAME}_gold"
MONITORING_SCHEMA = f"{PROJECT_NAME}_monitoring"

EXECUTION_ID = str(uuid4())
STARTED_AT = datetime.now(timezone.utc)
START_COUNTER = perf_counter()
EXECUTED_AT = datetime.now(timezone.utc)

quality_results = []

# COMMAND ----------

def table_exists(
    schema_name: str,
    table_name: str,
) -> bool:
    return spark.catalog.tableExists(
        f"{CATALOG}.{schema_name}.{table_name}"
    )


def add_result(
    layer: str,
    table_name: str,
    rule_name: str,
    rule_description: str,
    severity: str,
    invalid_records: int,
    total_records: int,
    details: str = "",
) -> None:
    invalid_records = int(invalid_records)
    total_records = int(total_records)

    conformity = (
        100.0
        if total_records == 0 and invalid_records == 0
        else round(
            (
                1
                - (
                    invalid_records
                    / max(total_records, 1)
                )
            )
            * 100,
            2,
        )
    )

    status = (
        "PASS"
        if invalid_records == 0
        else (
            "WARN"
            if severity == "WARNING"
            else "FAIL"
        )
    )

    quality_results.append(
        (
            EXECUTION_ID,
            layer,
            table_name,
            rule_name,
            rule_description,
            severity,
            status,
            invalid_records,
            total_records,
            conformity,
            details,
            EXECUTED_AT,
        )
    )


def check_non_empty(
    layer: str,
    schema_name: str,
    table_name: str,
) -> None:
    if not table_exists(schema_name, table_name):
        add_result(
            layer,
            table_name,
            "table_exists",
            "A tabela deve existir.",
            "ERROR",
            1,
            1,
            "Tabela não encontrada.",
        )
        return

    total = spark.table(
        f"{CATALOG}.{schema_name}.{table_name}"
    ).count()

    add_result(
        layer,
        table_name,
        "non_empty",
        "A tabela deve possuir ao menos um registro.",
        "ERROR",
        0 if total > 0 else 1,
        max(total, 1),
        f"Registros encontrados: {total}.",
    )


def check_nulls(
    layer: str,
    schema_name: str,
    table_name: str,
    columns: list[str],
) -> None:
    df = spark.table(
        f"{CATALOG}.{schema_name}.{table_name}"
    )

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    if not existing_columns:
        add_result(
            layer,
            table_name,
            "required_columns",
            "As colunas obrigatórias devem existir.",
            "ERROR",
            1,
            1,
            f"Colunas esperadas: {columns}.",
        )
        return

    total = df.count()

    invalid_condition = None

    for column in existing_columns:
        condition = F.col(column).isNull()
        invalid_condition = (
            condition
            if invalid_condition is None
            else invalid_condition | condition
        )

    invalid = df.filter(invalid_condition).count()

    add_result(
        layer,
        table_name,
        "completeness",
        (
            "Campos obrigatórios não podem ser nulos: "
            + ", ".join(existing_columns)
        ),
        "ERROR",
        invalid,
        total,
    )


def check_duplicates(
    layer: str,
    schema_name: str,
    table_name: str,
    keys: list[str],
) -> None:
    df = spark.table(
        f"{CATALOG}.{schema_name}.{table_name}"
    )

    existing_keys = [
        key for key in keys if key in df.columns
    ]

    if not existing_keys:
        add_result(
            layer,
            table_name,
            "business_key",
            "A chave de negócio deve existir.",
            "ERROR",
            1,
            1,
            f"Chaves esperadas: {keys}.",
        )
        return

    total = df.count()

    duplicate_groups = (
        df.groupBy(*existing_keys)
        .count()
        .filter(F.col("count") > 1)
    )

    invalid = (
        duplicate_groups
        .agg(
            F.sum(
                F.col("count") - F.lit(1)
            ).alias("duplicates")
        )
        .first()["duplicates"]
        or 0
    )

    add_result(
        layer,
        table_name,
        "uniqueness",
        (
            "A chave de negócio deve ser única: "
            + ", ".join(existing_keys)
        ),
        "ERROR",
        int(invalid),
        total,
    )


def check_percentage_domain(
    layer: str,
    schema_name: str,
    table_name: str,
    columns: list[str],
) -> None:
    df = spark.table(
        f"{CATALOG}.{schema_name}.{table_name}"
    )

    total = df.count()

    for column in columns:
        if column not in df.columns:
            continue

        invalid = (
            df.filter(
                F.col(column).isNotNull()
                & ~F.col(column).between(0.0, 100.0)
            )
            .count()
        )

        add_result(
            layer,
            table_name,
            f"domain_{column}",
            (
                f"{column} deve estar entre 0 e 100 "
                "quando preenchido."
            ),
            "ERROR",
            invalid,
            total,
        )

# COMMAND ----------

bronze_tables = [
    "alunos",
    "municipio",
    "uf",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "eventos_indicador_stream",
]

silver_tables = [
    "fato_alunos",
    "fato_municipio",
    "fato_uf",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "eventos_indicador",
    "dim_municipio",
    "dim_uf",
]

gold_tables = [
    "indicador_municipio",
    "resumo_uf",
    "resumo_brasil",
    "base_modelagem_municipio",
]

for table_name in bronze_tables:
    check_non_empty(
        "bronze",
        BRONZE_SCHEMA,
        table_name,
    )

for table_name in silver_tables:
    check_non_empty(
        "silver",
        SILVER_SCHEMA,
        table_name,
    )

for table_name in gold_tables:
    check_non_empty(
        "gold",
        GOLD_SCHEMA,
        table_name,
    )

# COMMAND ----------

check_nulls(
    "silver",
    SILVER_SCHEMA,
    "fato_alunos",
    ["ano", "id_municipio"],
)

check_nulls(
    "silver",
    SILVER_SCHEMA,
    "fato_municipio",
    ["ano", "id_municipio", "taxa_alfabetizacao"],
)

check_nulls(
    "silver",
    SILVER_SCHEMA,
    "fato_uf",
    ["ano", "sigla_uf", "taxa_alfabetizacao"],
)

check_nulls(
    "silver",
    SILVER_SCHEMA,
    "eventos_indicador",
    ["id_evento", "id_municipio", "ano"],
)

check_nulls(
    "gold",
    GOLD_SCHEMA,
    "indicador_municipio",
    ["ano", "id_municipio"],
)

# COMMAND ----------

check_duplicates(
    "silver",
    SILVER_SCHEMA,
    "eventos_indicador",
    ["id_evento"],
)

check_duplicates(
    "silver",
    SILVER_SCHEMA,
    "dim_municipio",
    ["id_municipio"],
)

check_duplicates(
    "silver",
    SILVER_SCHEMA,
    "dim_uf",
    ["sigla_uf"],
)

check_duplicates(
    "gold",
    GOLD_SCHEMA,
    "indicador_municipio",
    ["ano", "id_municipio", "rede"],
)

check_duplicates(
    "gold",
    GOLD_SCHEMA,
    "resumo_uf",
    ["ano", "sigla_uf", "rede"],
)

check_duplicates(
    "gold",
    GOLD_SCHEMA,
    "resumo_brasil",
    ["ano", "rede"],
)

# COMMAND ----------

check_percentage_domain(
    "silver",
    SILVER_SCHEMA,
    "fato_municipio",
    ["taxa_participacao", "taxa_alfabetizacao"],
)

check_percentage_domain(
    "silver",
    SILVER_SCHEMA,
    "fato_uf",
    ["taxa_participacao", "taxa_alfabetizacao"],
)

check_percentage_domain(
    "silver",
    SILVER_SCHEMA,
    "meta_alfabetizacao_municipio",
    ["meta_alfabetizacao"],
)

check_percentage_domain(
    "silver",
    SILVER_SCHEMA,
    "meta_alfabetizacao_uf",
    ["meta_alfabetizacao"],
)

check_percentage_domain(
    "silver",
    SILVER_SCHEMA,
    "meta_alfabetizacao_brasil",
    ["meta_alfabetizacao"],
)

check_percentage_domain(
    "gold",
    GOLD_SCHEMA,
    "indicador_municipio",
    [
        "taxa_alfabetizacao_oficial",
        "taxa_alfabetizacao_microdados",
        "taxa_alfabetizacao_streaming",
        "indicador_alfabetizacao_atual",
        "meta_alfabetizacao",
    ],
)

# COMMAND ----------

alunos = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.fato_alunos"
)

municipios = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.dim_municipio"
)

total_alunos = alunos.count()

orphan_students = (
    alunos.select("id_municipio")
    .distinct()
    .join(
        municipios.select("id_municipio").distinct(),
        on="id_municipio",
        how="left_anti",
    )
    .count()
)

add_result(
    "silver",
    "fato_alunos",
    "referential_integrity_municipio",
    (
        "Todo município presente em fato_alunos deve "
        "existir em dim_municipio."
    ),
    "ERROR",
    orphan_students,
    total_alunos,
)

metas_municipio = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}."
    "meta_alfabetizacao_municipio"
)

total_metas = metas_municipio.count()

orphan_targets = (
    metas_municipio.select("id_municipio")
    .distinct()
    .join(
        municipios.select("id_municipio").distinct(),
        on="id_municipio",
        how="left_anti",
    )
    .count()
)

add_result(
    "silver",
    "meta_alfabetizacao_municipio",
    "referential_integrity_municipio",
    (
        "Todo município presente nas metas deve existir "
        "em dim_municipio."
    ),
    "ERROR",
    orphan_targets,
    total_metas,
)

# COMMAND ----------

result_schema = StructType(
    [
        StructField("execution_id", StringType(), False),
        StructField("table_layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("rule_name", StringType(), False),
        StructField("rule_description", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("invalid_records", LongType(), False),
        StructField("total_records", LongType(), False),
        StructField(
            "conformity_percentage",
            DoubleType(),
            False,
        ),
        StructField("details", StringType(), True),
        StructField("executed_at", TimestampType(), False),
    ]
)

quality_df = spark.createDataFrame(
    quality_results,
    schema=result_schema,
)

(
    quality_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{MONITORING_SCHEMA}."
        "data_quality_results"
    )
)

display(
    quality_df.orderBy(
        F.when(F.col("status") == "FAIL", 1)
        .when(F.col("status") == "WARN", 2)
        .otherwise(3),
        "table_layer",
        "table_name",
        "rule_name",
    )
)

# COMMAND ----------

summary_df = (
    quality_df
    .groupBy("status", "severity")
    .agg(
        F.count("*").alias("quantidade_regras"),
        F.sum("invalid_records").alias(
            "registros_invalidos"
        ),
    )
    .orderBy("status", "severity")
)

display(summary_df)

failed_rules = quality_df.filter(
    F.col("status") == "FAIL"
).count()

ended_at = datetime.now(timezone.utc)
duration = perf_counter() - START_COUNTER

pipeline_status = (
    "WARNING"
    if failed_rules > 0
    else "SUCCESS"
)

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

(
    spark.createDataFrame(
        [
            (
                EXECUTION_ID,
                NOTEBOOK_NAME,
                "quality",
                STARTED_AT,
                ended_at,
                float(duration),
                pipeline_status,
                int(len(quality_results)),
                int(len(quality_results)),
                (
                    f"{len(quality_results)} regras executadas; "
                    f"{failed_rules} falharam."
                ),
                ended_at,
            )
        ],
        schema=log_schema,
    )
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{MONITORING_SCHEMA}.pipeline_runs"
    )
)

print(
    f"Qualidade concluída: {len(quality_results)} "
    f"regras, {failed_rules} falhas."
)
