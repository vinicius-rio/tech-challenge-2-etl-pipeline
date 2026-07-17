# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingestão streaming simulada para a camada Bronze
# MAGIC
# MAGIC Simula dois lotes de eventos em arquivos JSON e os processa
# MAGIC incrementalmente com Spark Structured Streaming.
# MAGIC
# MAGIC Em Databricks Serverless é utilizado `Trigger.AvailableNow`,
# MAGIC que processa todos os dados disponíveis e encerra a consulta.

# COMMAND ----------

from datetime import datetime, timedelta, timezone
from time import perf_counter
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_NAME = "alfabetizacao"
NOTEBOOK_NAME = "02_ingestao_streaming_bronze"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

BRONZE_SCHEMA = f"{PROJECT_NAME}_bronze"
MONITORING_SCHEMA = f"{PROJECT_NAME}_monitoring"

STREAMING_INPUT_PATH = (
    f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/streaming_input/events"
)

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/{MONITORING_SCHEMA}/"
    "checkpoints/eventos_indicador"
)

TARGET_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.eventos_indicador_stream"
)

RESET_DEMO = True
RUN_ID = str(uuid4())
STARTED_AT = datetime.now(timezone.utc)
START_COUNTER = perf_counter()

print(f"Input: {STREAMING_INPUT_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Destino: {TARGET_TABLE}")

# COMMAND ----------

if RESET_DEMO:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

    for path in [STREAMING_INPUT_PATH, CHECKPOINT_PATH]:
        try:
            dbutils.fs.rm(path, True)
        except Exception:
            pass

    print("Estado anterior da demonstração removido.")

# COMMAND ----------

def find_column(columns: list[str], candidates: list[str]):
    normalized = {column.lower(): column for column in columns}

    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]

    return None


municipio_table = (
    f"{CATALOG}.{BRONZE_SCHEMA}.municipio"
)

municipio_raw = spark.table(municipio_table)

id_column = find_column(
    municipio_raw.columns,
    ["id_municipio", "codigo_municipio"],
)
ano_column = find_column(
    municipio_raw.columns,
    ["ano", "ano_referencia"],
)
rede_column = find_column(
    municipio_raw.columns,
    ["rede", "dependencia_administrativa"],
)
taxa_column = find_column(
    municipio_raw.columns,
    [
        "taxa_alfabetizacao",
        "percentual_alfabetizado",
        "percentual_alfabetizacao",
        "indicador_alfabetizacao",
    ],
)

if not id_column:
    raise ValueError(
        "Não foi possível localizar a coluna de município "
        "na tabela Bronze."
    )

sample_df = (
    municipio_raw
    .select(
        F.trim(F.col(id_column)).alias("id_municipio"),
        (
            F.col(ano_column).cast("int")
            if ano_column
            else F.lit(2024)
        ).alias("ano"),
        (
            F.upper(F.trim(F.col(rede_column)))
            if rede_column
            else F.lit("TOTAL")
        ).alias("rede"),
        (
            F.regexp_replace(
                F.trim(F.col(taxa_column)),
                ",",
                ".",
            ).cast("double")
            if taxa_column
            else F.lit(65.0)
        ).alias("valor_base"),
    )
    .filter(F.col("id_municipio").isNotNull())
    .dropDuplicates(["id_municipio", "ano", "rede"])
    .limit(20)
)

sample_rows = sample_df.collect()

if not sample_rows:
    raise ValueError(
        "A tabela Bronze de município não possui linhas "
        "válidas para a simulação."
    )

# COMMAND ----------

event_schema = StructType(
    [
        StructField("id_evento", StringType(), False),
        StructField("id_municipio", StringType(), False),
        StructField("ano", IntegerType(), False),
        StructField("rede", StringType(), True),
        StructField("tipo_evento", StringType(), False),
        StructField("valor_indicador", DoubleType(), False),
        StructField("data_evento", TimestampType(), False),
        StructField("lote_origem", StringType(), False),
    ]
)


def build_events(
    rows,
    batch_number: int,
    value_adjustment: float,
):
    event_time = datetime.now(timezone.utc)
    events = []

    for index, row in enumerate(rows):
        base_value = (
            float(row["valor_base"])
            if row["valor_base"] is not None
            else 65.0
        )

        updated_value = max(
            0.0,
            min(
                100.0,
                base_value + value_adjustment + (index % 3) * 0.1,
            ),
        )

        events.append(
            (
                f"evt-{RUN_ID}-b{batch_number}-{index:04d}",
                row["id_municipio"],
                int(row["ano"] or 2024),
                row["rede"] or "TOTAL",
                "ATUALIZACAO_INDICADOR",
                float(round(updated_value, 2)),
                event_time + timedelta(seconds=index),
                f"batch_{batch_number}",
            )
        )

    return spark.createDataFrame(events, schema=event_schema)


def write_input_batch(batch_df, batch_number: int) -> int:
    count = batch_df.count()

    (
        batch_df
        .coalesce(1)
        .write
        .mode("append")
        .format("json")
        .save(STREAMING_INPUT_PATH)
    )

    print(
        f"Lote {batch_number} produzido: "
        f"{count} eventos JSON."
    )
    return int(count)


def run_available_now_stream() -> None:
    stream_source = (
        spark.readStream
        .schema(event_schema)
        .format("json")
        .load(STREAMING_INPUT_PATH)
    )

    enriched_stream = (
        stream_source
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_ingestion_date",
            F.current_date(),
        )
        .withColumn(
            "_source_system",
            F.lit("simulador_structured_streaming"),
        )
        .withColumn(
            "_ingestion_type",
            F.lit("streaming"),
        )
        .withColumn(
            "_source_file",
            F.lit("streaming_input_json"),
        )
    )

    query = (
        enriched_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .queryName("eventos_indicador_available_now")
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()

# COMMAND ----------

batch_1_df = build_events(
    sample_rows[:10],
    batch_number=1,
    value_adjustment=0.3,
)
batch_1_count = write_input_batch(batch_1_df, 1)
run_available_now_stream()

count_after_batch_1 = spark.table(TARGET_TABLE).count()

print(
    "Registros após o primeiro processamento: "
    f"{count_after_batch_1}"
)

# COMMAND ----------

batch_2_df = build_events(
    sample_rows[10:20] or sample_rows[:10],
    batch_number=2,
    value_adjustment=0.8,
)
batch_2_count = write_input_batch(batch_2_df, 2)
run_available_now_stream()

count_after_batch_2 = spark.table(TARGET_TABLE).count()

print(
    "Registros após o segundo processamento incremental: "
    f"{count_after_batch_2}"
)

# COMMAND ----------

display(
    spark.table(TARGET_TABLE)
    .orderBy(F.col("data_evento").desc())
)

# COMMAND ----------

ended_at = datetime.now(timezone.utc)
duration = perf_counter() - START_COUNTER
total_generated = batch_1_count + batch_2_count

log_data = [
    (
        RUN_ID,
        NOTEBOOK_NAME,
        "bronze_streaming",
        STARTED_AT,
        ended_at,
        float(duration),
        "SUCCESS",
        int(total_generated),
        int(count_after_batch_2),
        (
            "Dois lotes processados incrementalmente com "
            "Trigger.AvailableNow."
        ),
        ended_at,
    )
]

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
    spark.createDataFrame(log_data, schema=log_schema)
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{MONITORING_SCHEMA}.pipeline_runs"
    )
)

print("Simulação de streaming concluída com sucesso.")
