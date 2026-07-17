# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Consultas analíticas e evidências
# MAGIC
# MAGIC Apresenta consultas executivas para demonstrar o valor da camada
# MAGIC Gold e cria views reutilizáveis no Databricks SQL.

# COMMAND ----------

from pyspark.sql import functions as F

PROJECT_NAME = "alfabetizacao"

CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

GOLD_SCHEMA = f"{PROJECT_NAME}_gold"
SILVER_SCHEMA = f"{PROJECT_NAME}_silver"

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW
    `{CATALOG}`.`{GOLD_SCHEMA}`.`vw_ranking_uf` AS
    SELECT
        ano,
        sigla_uf,
        rede,
        indicador_alfabetizacao_uf,
        meta_alfabetizacao_uf,
        diferenca_para_meta_uf,
        status_meta_uf,
        quantidade_municipios,
        quantidade_alunos
    FROM
        `{CATALOG}`.`{GOLD_SCHEMA}`.`resumo_uf`
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW
    `{CATALOG}`.`{GOLD_SCHEMA}`.`vw_municipios_prioritarios` AS
    SELECT
        ano,
        id_municipio,
        nome_municipio,
        sigla_uf,
        rede,
        indicador_alfabetizacao_atual,
        meta_alfabetizacao,
        diferenca_para_meta,
        status_meta,
        quantidade_alunos,
        media_proficiencia_microdados
    FROM
        `{CATALOG}`.`{GOLD_SCHEMA}`.`indicador_municipio`
    WHERE status_meta = 'ABAIXO_DA_META'
    """
)

print("Views analíticas criadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Visão nacional

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            ano,
            rede,
            indicador_alfabetizacao_brasil,
            meta_alfabetizacao_brasil,
            diferenca_para_meta_brasil,
            status_meta_brasil,
            quantidade_ufs,
            quantidade_municipios,
            quantidade_alunos,
            ufs_meta_atingida,
            ufs_abaixo_meta
        FROM
            `{CATALOG}`.`{GOLD_SCHEMA}`.`resumo_brasil`
        ORDER BY ano, rede
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ranking das UFs por diferença para a meta

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM
            `{CATALOG}`.`{GOLD_SCHEMA}`.`vw_ranking_uf`
        WHERE indicador_alfabetizacao_uf IS NOT NULL
        ORDER BY
            ano DESC,
            diferenca_para_meta_uf DESC NULLS LAST
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Municípios prioritários

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM
            `{CATALOG}`.`{GOLD_SCHEMA}`.`vw_municipios_prioritarios`
        ORDER BY
            diferenca_para_meta ASC NULLS LAST,
            quantidade_alunos DESC NULLS LAST
        LIMIT 50
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Distribuição do cumprimento das metas

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            ano,
            sigla_uf,
            rede,
            status_meta,
            COUNT(DISTINCT id_municipio)
                AS quantidade_municipios
        FROM
            `{CATALOG}`.`{GOLD_SCHEMA}`.`indicador_municipio`
        GROUP BY
            ano,
            sigla_uf,
            rede,
            status_meta
        ORDER BY
            ano,
            sigla_uf,
            rede,
            status_meta
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparação entre indicador oficial e microdados

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            ano,
            sigla_uf,
            rede,
            ROUND(
                AVG(taxa_alfabetizacao_oficial),
                2
            ) AS media_indicador_oficial,
            ROUND(
                AVG(taxa_alfabetizacao_microdados),
                2
            ) AS media_microdados,
            ROUND(
                AVG(
                    taxa_alfabetizacao_microdados
                    - taxa_alfabetizacao_oficial
                ),
                2
            ) AS diferenca_media
        FROM
            `{CATALOG}`.`{GOLD_SCHEMA}`.`indicador_municipio`
        GROUP BY
            ano,
            sigla_uf,
            rede
        ORDER BY
            ano,
            sigla_uf,
            rede
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Eventos processados por streaming

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            lote_origem,
            COUNT(*) AS quantidade_eventos,
            MIN(data_evento) AS primeiro_evento,
            MAX(data_evento) AS ultimo_evento,
            COUNT(DISTINCT id_municipio)
                AS municipios_atualizados
        FROM
            `{CATALOG}`.`{SILVER_SCHEMA}`.`eventos_indicador`
        GROUP BY lote_origem
        ORDER BY lote_origem
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Base preparada para inteligência artificial

# COMMAND ----------

modeling_df = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}."
    "base_modelagem_municipio"
)

display(
    modeling_df.select(
        "ano",
        "id_municipio",
        "sigla_uf",
        "rede",
        "taxa_participacao_oficial",
        "media_proficiencia_microdados",
        "quantidade_alunos",
        "indicador_alfabetizacao_atual",
        "meta_alfabetizacao",
        "atingiu_meta",
    )
    .orderBy(
        F.col("quantidade_alunos").desc_nulls_last()
    )
    .limit(100)
)

# COMMAND ----------

display(
    modeling_df.groupBy(
        "ano",
        "sigla_uf",
        "atingiu_meta",
    )
    .agg(
        F.count("*").alias("observacoes"),
        F.round(
            F.avg("indicador_alfabetizacao_atual"),
            2,
        ).alias("media_indicador"),
        F.round(
            F.avg("media_proficiencia_microdados"),
            2,
        ).alias("media_proficiencia"),
    )
    .orderBy(
        "ano",
        "sigla_uf",
        "atingiu_meta",
    )
)

# COMMAND ----------

print(
    "Consultas analíticas concluídas. No Databricks, "
    "use o botão de visualização dos resultados para "
    "criar gráficos e capturar as evidências."
)
