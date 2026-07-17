# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Construção da camada Gold
# MAGIC
# MAGIC Cria datasets analíticos por município, UF e Brasil, além de uma
# MAGIC base de modelagem preparada para aplicações futuras de IA.

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
NOTEBOOK_NAME = "04_construcao_gold"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

SILVER_SCHEMA = f"{PROJECT_NAME}_silver"
GOLD_SCHEMA = f"{PROJECT_NAME}_gold"
MONITORING_SCHEMA = f"{PROJECT_NAME}_monitoring"

RUN_ID = str(uuid4())
STARTED_AT = datetime.now(timezone.utc)
START_COUNTER = perf_counter()

# COMMAND ----------

fato_alunos = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.fato_alunos"
)

fato_municipio = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.fato_municipio"
)

fato_uf = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.fato_uf"
)

meta_municipio = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}."
    "meta_alfabetizacao_municipio"
)

meta_uf = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}."
    "meta_alfabetizacao_uf"
)

meta_brasil = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}."
    "meta_alfabetizacao_brasil"
)

dim_municipio = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.dim_municipio"
    )
    .select(
        "id_municipio",
        "nome_municipio",
        F.col("sigla_uf").alias("dim_sigla_uf"),
    )
)

eventos = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.eventos_indicador"
)

# COMMAND ----------

student_aggregation = (
    fato_alunos
    .groupBy(
        "ano",
        "id_municipio",
        "sigla_uf",
        "rede",
    )
    .agg(
        F.count("*").alias("quantidade_alunos"),
        F.sum(
            F.when(
                F.col("presenca") == F.lit(True),
                1,
            ).otherwise(0)
        ).alias("quantidade_presentes"),
        F.round(
            F.avg("proficiencia"),
            2,
        ).alias("media_proficiencia_microdados"),
        F.round(
            F.avg(
                F.col("alfabetizado").cast("double")
            )
            * 100,
            2,
        ).alias("taxa_alfabetizacao_microdados"),
        F.round(
            F.sum(
                F.coalesce(
                    F.col("peso_aluno"),
                    F.lit(0.0),
                )
            ),
            2,
        ).alias("soma_pesos_alunos"),
    )
)

municipio_official = (
    fato_municipio
    .groupBy(
        "ano",
        "id_municipio",
        "sigla_uf",
        "rede",
    )
    .agg(
        F.round(
            F.avg("taxa_participacao"),
            2,
        ).alias("taxa_participacao_oficial"),
        F.round(
            F.avg("taxa_alfabetizacao"),
            2,
        ).alias("taxa_alfabetizacao_oficial"),
        F.round(
            F.avg("media_proficiencia"),
            2,
        ).alias("media_proficiencia_oficial"),
    )
)

municipal_targets = (
    meta_municipio
    .groupBy(
        F.col("ano_meta").alias("ano"),
        "id_municipio",
        "rede",
    )
    .agg(
        F.max("meta_alfabetizacao").alias(
            "meta_alfabetizacao"
        ),
        F.first(
            "nivel_alfabetizacao",
            ignorenulls=True,
        ).alias("nivel_alfabetizacao"),
    )
)

latest_events = (
    eventos
    .groupBy(
        "ano",
        "id_municipio",
        "rede",
    )
    .agg(
        F.max_by(
            "valor_indicador",
            "data_evento",
        ).alias("taxa_alfabetizacao_streaming"),
        F.max("data_evento").alias(
            "data_ultimo_evento"
        ),
    )
)

# COMMAND ----------

indicador_municipio = (
    municipio_official.alias("official")
    .join(
        student_aggregation.alias("students"),
        on=[
            "ano",
            "id_municipio",
            "sigla_uf",
            "rede",
        ],
        how="full",
    )
    .join(
        dim_municipio.alias("dim"),
        on=["id_municipio"],
        how="left",
    )
    .join(
        municipal_targets.alias("target"),
        on=["ano", "id_municipio", "rede"],
        how="left",
    )
    .join(
        latest_events.alias("stream"),
        on=["ano", "id_municipio", "rede"],
        how="left",
    )
    .select(
        F.col("ano"),
        F.col("id_municipio"),
        F.coalesce(
            F.col("sigla_uf"),
            F.col("dim_sigla_uf"),
        ).alias("sigla_uf"),
        F.col("dim.nome_municipio"),
        F.col("rede"),
        F.col("taxa_participacao_oficial"),
        F.col("taxa_alfabetizacao_oficial"),
        F.col("taxa_alfabetizacao_microdados"),
        F.col("taxa_alfabetizacao_streaming"),
        F.coalesce(
            F.col("taxa_alfabetizacao_streaming"),
            F.col("taxa_alfabetizacao_oficial"),
            F.col("taxa_alfabetizacao_microdados"),
        ).alias("indicador_alfabetizacao_atual"),
        F.col("meta_alfabetizacao"),
        F.col("nivel_alfabetizacao"),
        F.col("quantidade_alunos"),
        F.col("quantidade_presentes"),
        F.col("media_proficiencia_oficial"),
        F.col("media_proficiencia_microdados"),
        F.col("soma_pesos_alunos"),
        F.col("data_ultimo_evento"),
    )
    .withColumn(
        "diferenca_para_meta",
        F.round(
            F.col("indicador_alfabetizacao_atual")
            - F.col("meta_alfabetizacao"),
            2,
        ),
    )
    .withColumn(
        "status_meta",
        F.when(
            F.col("meta_alfabetizacao").isNull(),
            F.lit("SEM_META"),
        )
        .when(
            F.col("indicador_alfabetizacao_atual")
            >= F.col("meta_alfabetizacao"),
            F.lit("META_ATINGIDA"),
        )
        .otherwise(F.lit("ABAIXO_DA_META")),
    )
    .withColumn(
        "data_processamento",
        F.current_timestamp(),
    )
    .withColumn(
        "_gold_run_id",
        F.lit(RUN_ID),
    )
)

# COMMAND ----------

municipal_summary = (
    indicador_municipio
    .groupBy(
        "ano",
        "sigla_uf",
        "rede",
    )
    .agg(
        F.countDistinct("id_municipio").alias(
            "quantidade_municipios"
        ),
        F.round(
            F.avg("indicador_alfabetizacao_atual"),
            2,
        ).alias("media_municipal_alfabetizacao"),
        F.sum(
            F.coalesce(
                F.col("quantidade_alunos"),
                F.lit(0),
            )
        ).alias("quantidade_alunos"),
        F.sum(
            F.when(
                F.col("status_meta") == "META_ATINGIDA",
                1,
            ).otherwise(0)
        ).alias("municipios_meta_atingida"),
        F.sum(
            F.when(
                F.col("status_meta") == "ABAIXO_DA_META",
                1,
            ).otherwise(0)
        ).alias("municipios_abaixo_meta"),
    )
)

uf_official = (
    fato_uf
    .groupBy(
        "ano",
        "sigla_uf",
        "rede",
    )
    .agg(
        F.round(
            F.avg("taxa_alfabetizacao"),
            2,
        ).alias("taxa_alfabetizacao_oficial_uf"),
        F.round(
            F.avg("taxa_participacao"),
            2,
        ).alias("taxa_participacao_oficial_uf"),
        F.round(
            F.avg("media_proficiencia"),
            2,
        ).alias("media_proficiencia_oficial_uf"),
    )
)

uf_targets = (
    meta_uf
    .groupBy(
        F.col("ano_meta").alias("ano"),
        "sigla_uf",
        "rede",
    )
    .agg(
        F.max("meta_alfabetizacao").alias(
            "meta_alfabetizacao_uf"
        )
    )
)

resumo_uf = (
    uf_official
    .join(
        municipal_summary,
        on=["ano", "sigla_uf", "rede"],
        how="full",
    )
    .join(
        uf_targets,
        on=["ano", "sigla_uf", "rede"],
        how="left",
    )
    .withColumn(
        "indicador_alfabetizacao_uf",
        F.coalesce(
            F.col("taxa_alfabetizacao_oficial_uf"),
            F.col("media_municipal_alfabetizacao"),
        ),
    )
    .withColumn(
        "diferenca_para_meta_uf",
        F.round(
            F.col("indicador_alfabetizacao_uf")
            - F.col("meta_alfabetizacao_uf"),
            2,
        ),
    )
    .withColumn(
        "status_meta_uf",
        F.when(
            F.col("meta_alfabetizacao_uf").isNull(),
            F.lit("SEM_META"),
        )
        .when(
            F.col("indicador_alfabetizacao_uf")
            >= F.col("meta_alfabetizacao_uf"),
            F.lit("META_ATINGIDA"),
        )
        .otherwise(F.lit("ABAIXO_DA_META")),
    )
    .withColumn(
        "data_processamento",
        F.current_timestamp(),
    )
    .withColumn(
        "_gold_run_id",
        F.lit(RUN_ID),
    )
)

# COMMAND ----------

national_aggregation = (
    resumo_uf
    .groupBy("ano", "rede")
    .agg(
        F.countDistinct("sigla_uf").alias(
            "quantidade_ufs"
        ),
        F.sum(
            F.coalesce(
                F.col("quantidade_municipios"),
                F.lit(0),
            )
        ).alias("quantidade_municipios"),
        F.sum(
            F.coalesce(
                F.col("quantidade_alunos"),
                F.lit(0),
            )
        ).alias("quantidade_alunos"),
        F.round(
            F.avg("indicador_alfabetizacao_uf"),
            2,
        ).alias("indicador_alfabetizacao_brasil"),
        F.sum(
            F.when(
                F.col("status_meta_uf") == "META_ATINGIDA",
                1,
            ).otherwise(0)
        ).alias("ufs_meta_atingida"),
        F.sum(
            F.when(
                F.col("status_meta_uf") == "ABAIXO_DA_META",
                1,
            ).otherwise(0)
        ).alias("ufs_abaixo_meta"),
    )
)

brasil_targets = (
    meta_brasil
    .groupBy(
        F.col("ano_meta").alias("ano"),
        "rede",
    )
    .agg(
        F.max("meta_alfabetizacao").alias(
            "meta_alfabetizacao_brasil"
        )
    )
)

resumo_brasil = (
    national_aggregation
    .join(
        brasil_targets,
        on=["ano", "rede"],
        how="left",
    )
    .withColumn(
        "diferenca_para_meta_brasil",
        F.round(
            F.col("indicador_alfabetizacao_brasil")
            - F.col("meta_alfabetizacao_brasil"),
            2,
        ),
    )
    .withColumn(
        "status_meta_brasil",
        F.when(
            F.col("meta_alfabetizacao_brasil").isNull(),
            F.lit("SEM_META"),
        )
        .when(
            F.col("indicador_alfabetizacao_brasil")
            >= F.col("meta_alfabetizacao_brasil"),
            F.lit("META_ATINGIDA"),
        )
        .otherwise(F.lit("ABAIXO_DA_META")),
    )
    .withColumn(
        "data_processamento",
        F.current_timestamp(),
    )
    .withColumn(
        "_gold_run_id",
        F.lit(RUN_ID),
    )
)

# COMMAND ----------

base_modelagem_municipio = (
    indicador_municipio
    .select(
        "ano",
        "id_municipio",
        "sigla_uf",
        "rede",
        "taxa_participacao_oficial",
        "taxa_alfabetizacao_oficial",
        "taxa_alfabetizacao_microdados",
        "indicador_alfabetizacao_atual",
        "meta_alfabetizacao",
        "diferenca_para_meta",
        "status_meta",
        "quantidade_alunos",
        "quantidade_presentes",
        "media_proficiencia_oficial",
        "media_proficiencia_microdados",
    )
    .withColumn(
        "atingiu_meta",
        F.when(
            F.col("status_meta") == "META_ATINGIDA",
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "data_processamento",
        F.current_timestamp(),
    )
    .withColumn(
        "_gold_run_id",
        F.lit(RUN_ID),
    )
)

# COMMAND ----------

def write_gold(
    df: DataFrame,
    table_name: str,
) -> int:
    full_table_name = (
        f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
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


outputs = {
    "indicador_municipio": indicador_municipio,
    "resumo_uf": resumo_uf,
    "resumo_brasil": resumo_brasil,
    "base_modelagem_municipio": (
        base_modelagem_municipio
    ),
}

table_results = []
written_rows = 0

for table_name, dataframe in outputs.items():
    row_count = write_gold(
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

(
    spark.createDataFrame(
        [
            (
                RUN_ID,
                NOTEBOOK_NAME,
                "gold",
                STARTED_AT,
                ended_at,
                float(duration),
                "SUCCESS",
                int(written_rows),
                int(written_rows),
                (
                    f"{len(outputs)} tabelas Gold "
                    "criadas com sucesso."
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

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{CATALOG}`.`{GOLD_SCHEMA}`"
    )
)

display(
    spark.table(
        f"{CATALOG}.{GOLD_SCHEMA}.indicador_municipio"
    )
    .orderBy(
        F.col("diferenca_para_meta").asc_nulls_last()
    )
    .limit(20)
)

print("Construção da camada Gold concluída.")
