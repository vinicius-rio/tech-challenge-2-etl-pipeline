# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Configuração inicial do projeto
# MAGIC
# MAGIC Este notebook prepara a estrutura lógica da pipeline:
# MAGIC
# MAGIC - camada Bronze;
# MAGIC - camada Silver;
# MAGIC - camada Gold;
# MAGIC - camada de monitoramento;
# MAGIC - Volume para arquivos de entrada;
# MAGIC - Volume para checkpoints de streaming.

# COMMAND ----------

from datetime import datetime, timezone

PROJECT_NAME = "alfabetizacao"

# Utiliza o catálogo atual do workspace.
# Isso evita depender da permissão de criação de um novo catálogo.
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
print(f"Data da configuração: {datetime.now(timezone.utc).isoformat()}")

for layer, schema_name in SCHEMAS.items():
    print(f"{layer}: {CATALOG}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação dos schemas

# COMMAND ----------

SCHEMA_COMMENTS = {
    "bronze": "Dados brutos ingeridos das fontes batch e streaming.",
    "silver": "Dados tratados, padronizados, validados e integrados.",
    "gold": "Datasets analíticos preparados para consultas e visualizações.",
    "monitoring": "Métricas de execução, qualidade e observabilidade da pipeline.",
}

for layer, schema_name in SCHEMAS.items():
    comment = SCHEMA_COMMENTS[layer]

    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema_name}`
        COMMENT '{comment}'
        """
    )

    print(f"Schema criado ou validado: {CATALOG}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação dos Volumes

# COMMAND ----------

VOLUMES = {
    "landing": {
        "schema": SCHEMAS["bronze"],
        "description": "Arquivos originais recebidos das fontes de dados.",
    },
    "checkpoints": {
        "schema": SCHEMAS["monitoring"],
        "description": "Checkpoints utilizados pelo Spark Structured Streaming.",
    },
}

for volume_name, config in VOLUMES.items():
    schema_name = config["schema"]

    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS
        `{CATALOG}`.`{schema_name}`.`{volume_name}`
        COMMENT '{config["description"]}'
        """
    )

    print(
        "Volume criado ou validado: "
        f"{CATALOG}.{schema_name}.{volume_name}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caminhos utilizados pela pipeline

# COMMAND ----------

LANDING_PATH = (
    f"/Volumes/{CATALOG}/"
    f"{SCHEMAS['bronze']}/landing"
)

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/"
    f"{SCHEMAS['monitoring']}/checkpoints"
)

print(f"Landing path: {LANDING_PATH}")
print(f"Checkpoint path: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos schemas criados

# COMMAND ----------

schemas_df = spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`")

display(
    schemas_df.filter(
        schemas_df.databaseName.isin(list(SCHEMAS.values()))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Volumes

# COMMAND ----------

display(
    spark.sql(
        f"""
        SHOW VOLUMES IN
        `{CATALOG}`.`{SCHEMAS['bronze']}`
        """
    )
)

display(
    spark.sql(
        f"""
        SHOW VOLUMES IN
        `{CATALOG}`.`{SCHEMAS['monitoring']}`
        """
    )
)

# COMMAND ----------

print("Configuração inicial concluída com sucesso.")