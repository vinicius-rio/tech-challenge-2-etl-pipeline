# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Configuração inicial do projeto
# MAGIC
# MAGIC Cria a estrutura lógica da Arquitetura Medalhão, os Volumes usados
# MAGIC pela ingestão e as tabelas de monitoramento da pipeline.

# COMMAND ----------

from datetime import datetime, timezone

PROJECT_NAME = "alfabetizacao"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

SCHEMAS = {
    "bronze": f"{PROJECT_NAME}_bronze",
    "silver": f"{PROJECT_NAME}_silver",
    "gold": f"{PROJECT_NAME}_gold",
    "monitoring": f"{PROJECT_NAME}_monitoring",
}

print(f"Catálogo utilizado: {CATALOG}")
print(f"Execução iniciada em: {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

SCHEMA_COMMENTS = {
    "bronze": "Dados brutos ingeridos por processos batch e streaming.",
    "silver": "Dados limpos, tipados, padronizados, validados e integrados.",
    "gold": "Datasets analíticos preparados para consumo e inteligência artificial.",
    "monitoring": "Métricas operacionais, qualidade e observabilidade da pipeline.",
}

for layer, schema_name in SCHEMAS.items():
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema_name}`
        COMMENT '{SCHEMA_COMMENTS[layer]}'
        """
    )
    print(f"Schema validado: {CATALOG}.{schema_name}")

# COMMAND ----------

VOLUMES = [
    {
        "schema": SCHEMAS["bronze"],
        "name": "landing",
        "comment": "Arquivos históricos recebidos da Base dos Dados.",
    },
    {
        "schema": SCHEMAS["bronze"],
        "name": "streaming_input",
        "comment": "Arquivos JSON que simulam a chegada de eventos em tempo quase real.",
    },
    {
        "schema": SCHEMAS["monitoring"],
        "name": "checkpoints",
        "comment": "Checkpoints do Spark Structured Streaming.",
    },
]

for volume in VOLUMES:
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS
        `{CATALOG}`.`{volume["schema"]}`.`{volume["name"]}`
        COMMENT '{volume["comment"]}'
        """
    )
    print(
        "Volume validado: "
        f'{CATALOG}.{volume["schema"]}.{volume["name"]}'
    )

# COMMAND ----------

MONITORING_SCHEMA = SCHEMAS["monitoring"]

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{CATALOG}`.`{MONITORING_SCHEMA}`.`pipeline_runs` (
        run_id STRING,
        notebook STRING,
        layer STRING,
        start_timestamp TIMESTAMP,
        end_timestamp TIMESTAMP,
        duration_seconds DOUBLE,
        status STRING,
        records_read BIGINT,
        records_written BIGINT,
        message STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Histórico de execuções dos notebooks da pipeline.'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{CATALOG}`.`{MONITORING_SCHEMA}`.`data_quality_results` (
        execution_id STRING,
        table_layer STRING,
        table_name STRING,
        rule_name STRING,
        rule_description STRING,
        severity STRING,
        status STRING,
        invalid_records BIGINT,
        total_records BIGINT,
        conformity_percentage DOUBLE,
        details STRING,
        executed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Resultados das regras de qualidade de dados.'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{CATALOG}`.`{MONITORING_SCHEMA}`.`table_metrics` (
        execution_id STRING,
        table_layer STRING,
        table_name STRING,
        row_count BIGINT,
        column_count INT,
        num_files BIGINT,
        size_in_bytes BIGINT,
        collected_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Snapshot de volume, quantidade de colunas e tamanho físico das tabelas.'
    """
)

print("Tabelas de monitoramento criadas ou validadas.")

# COMMAND ----------

LANDING_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMAS['bronze']}/landing"
)

STREAMING_INPUT_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMAS['bronze']}/streaming_input"
)

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMAS['monitoring']}/checkpoints"
)

print(f"Landing: {LANDING_PATH}")
print(f"Streaming input: {STREAMING_INPUT_PATH}")
print(f"Checkpoints: {CHECKPOINT_PATH}")

# COMMAND ----------

display(
    spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`")
    .filter(f"databaseName LIKE '{PROJECT_NAME}_%'")
)

display(
    spark.sql(
        f"SHOW VOLUMES IN `{CATALOG}`.`{SCHEMAS['bronze']}`"
    )
)

display(
    spark.sql(
        f"SHOW VOLUMES IN `{CATALOG}`.`{SCHEMAS['monitoring']}`"
    )
)

# COMMAND ----------

print("Configuração inicial concluída com sucesso.")
