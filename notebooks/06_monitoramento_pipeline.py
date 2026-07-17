# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Monitoramento e observabilidade
# MAGIC
# MAGIC Consolida histórico de execuções, qualidade, volume de registros,
# MAGIC arquivos Delta e armazenamento utilizado pelas camadas.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_NAME = "alfabetizacao"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

SCHEMAS = {
    "bronze": f"{PROJECT_NAME}_bronze",
    "silver": f"{PROJECT_NAME}_silver",
    "gold": f"{PROJECT_NAME}_gold",
}

MONITORING_SCHEMA = f"{PROJECT_NAME}_monitoring"
EXECUTION_ID = str(uuid4())
COLLECTED_AT = datetime.now(timezone.utc)

# COMMAND ----------

metric_rows = []

for layer, schema_name in SCHEMAS.items():
    tables = (
        spark.sql(
            f"SHOW TABLES IN `{CATALOG}`.`{schema_name}`"
        )
        .select("tableName")
        .collect()
    )

    for table_row in tables:
        table_name = table_row["tableName"]
        full_table_name = (
            f"{CATALOG}.{schema_name}.{table_name}"
        )

        table_df = spark.table(full_table_name)
        row_count = int(table_df.count())
        column_count = int(len(table_df.columns))

        num_files = None
        size_in_bytes = None

        try:
            detail = spark.sql(
                f"DESCRIBE DETAIL {full_table_name}"
            ).first()

            num_files = (
                int(detail["numFiles"])
                if detail["numFiles"] is not None
                else None
            )
            size_in_bytes = (
                int(detail["sizeInBytes"])
                if detail["sizeInBytes"] is not None
                else None
            )

        except Exception as error:
            print(
                f"DESCRIBE DETAIL indisponível para "
                f"{full_table_name}: {error}"
            )

        metric_rows.append(
            (
                EXECUTION_ID,
                layer,
                table_name,
                row_count,
                column_count,
                num_files,
                size_in_bytes,
                COLLECTED_AT,
            )
        )

# COMMAND ----------

metric_schema = StructType(
    [
        StructField("execution_id", StringType(), False),
        StructField("table_layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("column_count", IntegerType(), False),
        StructField("num_files", LongType(), True),
        StructField("size_in_bytes", LongType(), True),
        StructField("collected_at", TimestampType(), False),
    ]
)

metrics_df = spark.createDataFrame(
    metric_rows,
    schema=metric_schema,
)

(
    metrics_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.{MONITORING_SCHEMA}.table_metrics"
    )
)

display(
    metrics_df
    .withColumn(
        "size_mb",
        F.round(
            F.col("size_in_bytes")
            / F.lit(1024 * 1024),
            2,
        ),
    )
    .orderBy(
        "table_layer",
        F.col("row_count").desc(),
    )
)

# COMMAND ----------

display(
    spark.table(
        f"{CATALOG}.{MONITORING_SCHEMA}."
        "pipeline_runs"
    )
    .orderBy(F.col("start_timestamp").desc())
)

# COMMAND ----------

latest_quality_execution = (
    spark.table(
        f"{CATALOG}.{MONITORING_SCHEMA}."
        "data_quality_results"
    )
    .agg(
        F.max("executed_at").alias("latest")
    )
    .first()["latest"]
)

if latest_quality_execution:
    latest_quality = (
        spark.table(
            f"{CATALOG}.{MONITORING_SCHEMA}."
            "data_quality_results"
        )
        .filter(
            F.col("executed_at")
            == F.lit(latest_quality_execution)
        )
    )

    display(
        latest_quality
        .groupBy("status", "severity")
        .agg(
            F.count("*").alias("quantidade_regras"),
            F.sum("invalid_records").alias(
                "registros_invalidos"
            ),
        )
        .orderBy("status", "severity")
    )

    display(
        latest_quality
        .filter(F.col("status") != "PASS")
        .orderBy(
            "status",
            "table_layer",
            "table_name",
        )
    )

# COMMAND ----------

finops_summary = (
    metrics_df
    .groupBy("table_layer")
    .agg(
        F.sum("row_count").alias("registros"),
        F.sum("num_files").alias("arquivos_delta"),
        F.sum("size_in_bytes").alias(
            "tamanho_total_bytes"
        ),
    )
    .withColumn(
        "tamanho_total_mb",
        F.round(
            F.col("tamanho_total_bytes")
            / F.lit(1024 * 1024),
            2,
        ),
    )
    .orderBy("table_layer")
)

display(finops_summary)

# COMMAND ----------

stream_table = (
    f"{CATALOG}.{SCHEMAS['bronze']}."
    "eventos_indicador_stream"
)

if spark.catalog.tableExists(stream_table):
    print("Histórico Delta da tabela de streaming:")
    display(
        spark.sql(
            f"DESCRIBE HISTORY {stream_table}"
        ).limit(20)
    )

# COMMAND ----------

print(
    "Monitoramento concluído. Use as tabelas e os "
    "gráficos desta execução como evidência no PPT."
)
