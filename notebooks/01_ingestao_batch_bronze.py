# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão batch para a camada Bronze
# MAGIC
# MAGIC Lê os seis arquivos históricos do Volume `landing`, preserva as
# MAGIC colunas originais como texto, adiciona metadados técnicos e grava
# MAGIC tabelas Delta gerenciadas na camada Bronze.
# MAGIC
# MAGIC O código é compatível com Databricks Serverless e não utiliza
# MAGIC `persist()`, `cache()` ou `unpersist()`.

# COMMAND ----------

from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

PROJECT_NAME = "alfabetizacao"
NOTEBOOK_NAME = "01_ingestao_batch_bronze"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

BRONZE_SCHEMA = f"{PROJECT_NAME}_bronze"
MONITORING_SCHEMA = f"{PROJECT_NAME}_monitoring"

LANDING_PATH = (
    f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"
)

RUN_ID = str(uuid4())
BATCH_ID = datetime.now(timezone.utc).strftime(
    "%Y%m%dT%H%M%S%fZ"
)
STARTED_AT = datetime.now(timezone.utc)
START_COUNTER = perf_counter()

DATASETS = {
    "alunos": "alunos.csv",
    "meta_alfabetizacao_brasil": (
        "br_inep_avaliacao_alfabetizacao_"
        "meta_alfabetizacao_brasil.csv.gz"
    ),
    "meta_alfabetizacao_municipio": (
        "br_inep_avaliacao_alfabetizacao_"
        "meta_alfabetizacao_municipio.csv.gz"
    ),
    "meta_alfabetizacao_uf": (
        "br_inep_avaliacao_alfabetizacao_"
        "meta_alfabetizacao_uf.csv.gz"
    ),
    "municipio": (
        "br_inep_avaliacao_alfabetizacao_"
        "municipio.csv.gz"
    ),
    "uf": (
        "br_inep_avaliacao_alfabetizacao_"
        "uf.csv.gz"
    ),
}

print(f"Catálogo: {CATALOG}")
print(f"Landing: {LANDING_PATH}")
print(f"Run ID: {RUN_ID}")
print(f"Batch ID: {BATCH_ID}")

# COMMAND ----------

available_file_info = [
    file_info
    for file_info in dbutils.fs.ls(LANDING_PATH)
    if not file_info.isDir()
]

available_files = {
    file_info.name
    for file_info in available_file_info
}

expected_files = set(DATASETS.values())
missing_files = expected_files - available_files
unexpected_files = available_files - expected_files

print("Arquivos encontrados no Volume:")

for file_info in sorted(
    available_file_info,
    key=lambda item: item.name,
):
    size_mb = file_info.size / (1024 ** 2)
    print(f"- {file_info.name}: {size_mb:.2f} MB")

if unexpected_files:
    print("\nArquivos adicionais, que não serão ingeridos:")
    for file_name in sorted(unexpected_files):
        print(f"- {file_name}")

if missing_files:
    missing_list = ", ".join(sorted(missing_files))
    raise FileNotFoundError(
        "Arquivos obrigatórios ausentes no Volume: "
        f"{missing_list}"
    )

print("\nTodos os seis arquivos obrigatórios foram encontrados.")

# COMMAND ----------

def read_csv_as_raw(file_name: str) -> DataFrame:
    """
    Lê CSV ou CSV.GZ sem inferir tipos.

    A camada Bronze preserva os valores recebidos e posterga
    conversões de tipo e regras de negócio para a Silver.
    """

    file_path = f"{LANDING_PATH}/{file_name}"

    raw_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("quote", '"')
        .option("escape", '"')
        .load(file_path)
    )

    return (
        raw_df
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_ingestion_date",
            F.current_date(),
        )
        .withColumn(
            "_batch_id",
            F.lit(BATCH_ID),
        )
        .withColumn(
            "_source_file",
            F.lit(file_name),
        )
        .withColumn(
            "_source_system",
            F.lit("Base dos Dados"),
        )
        .withColumn(
            "_ingestion_type",
            F.lit("batch"),
        )
    )


def log_pipeline_run(
    status: str,
    records_read: int,
    records_written: int,
    message: str,
) -> None:
    ended_at = datetime.now(timezone.utc)
    duration = perf_counter() - START_COUNTER

    log_schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("notebook", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("start_timestamp", StringType(), False),
            StructField("end_timestamp", StringType(), False),
            StructField("duration_seconds", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_read", LongType(), False),
            StructField("records_written", LongType(), False),
            StructField("message", StringType(), True),
            StructField("created_at", StringType(), False),
        ]
    )

    log_df = spark.createDataFrame(
        [
            (
                RUN_ID,
                NOTEBOOK_NAME,
                "bronze",
                STARTED_AT.isoformat(),
                ended_at.isoformat(),
                str(float(duration)),
                status,
                int(records_read),
                int(records_written),
                message,
                ended_at.isoformat(),
            )
        ],
        schema=log_schema,
    ).select(
        "run_id",
        "notebook",
        "layer",
        F.to_timestamp("start_timestamp").alias("start_timestamp"),
        F.to_timestamp("end_timestamp").alias("end_timestamp"),
        F.col("duration_seconds").cast("double").alias(
            "duration_seconds"
        ),
        "status",
        "records_read",
        "records_written",
        "message",
        F.to_timestamp("created_at").alias("created_at"),
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

result_schema = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("column_count", LongType(), False),
        StructField("batch_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

ingestion_results = []
errors = []

for table_name, file_name in DATASETS.items():
    full_table_name = (
        f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}`"
    )

    print("=" * 100)
    print(f"Iniciando ingestão de {file_name}")
    print(f"Destino: {full_table_name}")

    try:
        bronze_df = read_csv_as_raw(file_name)
        column_count = len(bronze_df.columns)

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )

        row_count = int(
            spark.sql(
                f"""
                SELECT COUNT(*) AS row_count
                FROM {full_table_name}
                """
            ).first()["row_count"]
        )

        ingestion_results.append(
            (
                table_name,
                file_name,
                row_count,
                column_count,
                BATCH_ID,
                "SUCCESS",
                None,
            )
        )

        print(
            f"Sucesso: {row_count:,} registros e "
            f"{column_count} colunas."
        )

    except Exception as error:
        error_message = str(error)[:1000]
        errors.append(f"{table_name}: {error_message}")

        ingestion_results.append(
            (
                table_name,
                file_name,
                0,
                0,
                BATCH_ID,
                "ERROR",
                error_message,
            )
        )

        print(f"Erro na ingestão de {table_name}: {error_message}")

# COMMAND ----------

results_df = spark.createDataFrame(
    ingestion_results,
    schema=result_schema,
)

display(results_df.orderBy("table_name"))

total_records = int(
    results_df
    .agg(F.sum("row_count").alias("total"))
    .first()["total"]
    or 0
)

status = "ERROR" if errors else "SUCCESS"
message = (
    " | ".join(errors)
    if errors
    else "As seis tabelas batch foram ingeridas com sucesso."
)

log_pipeline_run(
    status=status,
    records_read=total_records,
    records_written=total_records,
    message=message,
)

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{CATALOG}`.`{BRONZE_SCHEMA}`"
    )
)

# COMMAND ----------

for table_name in DATASETS:
    full_table_name = (
        f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}`"
    )

    print(f"Amostra: {table_name}")
    display(spark.table(full_table_name).limit(5))

# COMMAND ----------

if errors:
    raise RuntimeError(
        "A ingestão batch terminou com erro em uma ou mais "
        "tabelas. Consulte o resumo exibido."
    )

print(
    f"Ingestão batch concluída. Total: "
    f"{total_records:,} registros."
)
