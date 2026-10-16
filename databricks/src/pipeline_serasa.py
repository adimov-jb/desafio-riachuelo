# Databricks notebook source
# Lakeflow Declarative Pipeline (DLT): Bronze e Silver do feed diário Serasa.
#
# Bronze  : CSV bruto como string, append-only, com metadados de arquivo e ingestão.
#           Único lugar com CPF em claro -> acesso restrito ao service principal de ingestão.
# Silver  : tipado, validado (expectations), CPF tokenizado (HMAC-like: sha2 + pepper),
#           deduplicado e HISTÓRICO COMPLETO (1 linha por cpf_token x data_referencia x versão).
#           A coluna `disponivel_em` (momento em que o dado chegou) é a base do point-in-time.

import dlt
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")
CPF_PEPPER = spark.conf.get("cpf_pepper")

COLUNAS = "cpf string, score_mercado string, faixa_renda_estimada string, propensao_credito string, data_referencia string"

# COMMAND ----------


@dlt.table(
    name="bronze_serasa_score",
    comment="Feed diário Serasa, bruto (strings). Contém CPF em claro: acesso restrito.",
    table_properties={"quality": "bronze", "contem_pii": "true", "delta.logRetentionDuration": "interval 90 days"},
)
def bronze_serasa_score():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .schema(COLUNAS)
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_arquivo"),
            F.col("_metadata.file_modification_time").alias("_arquivo_modificado_em"),
            F.current_timestamp().alias("_ingerido_em"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="silver_serasa_score",
    comment="Score Serasa tipado, validado e com CPF tokenizado. Histórico completo para point-in-time.",
    table_properties={"quality": "silver", "contem_pii": "false"},
    cluster_by=["cpf_token", "data_referencia"],
)
@dlt.expect_or_drop("cpf_com_11_digitos", "cpf_formato_ok")
@dlt.expect_or_drop("score_na_escala_0_1000", "score_mercado BETWEEN 0 AND 1000")
@dlt.expect_or_drop("data_referencia_valida", "data_referencia IS NOT NULL")
@dlt.expect("propensao_no_dominio", "propensao_credito IN ('Alta', 'Media', 'Baixa')")
@dlt.expect("layout_sem_colunas_inesperadas", "_rescued_data IS NULL")
def silver_serasa_score():
    cpf_digitos = F.lpad(F.regexp_replace("cpf", r"\D", ""), 11, "0")
    propensao = F.initcap(F.translate(F.lower(F.trim("propensao_credito")), "áéíóúâêôãõç", "aeiouaeoaoc"))
    return (
        dlt.read_stream("bronze_serasa_score")
        .select(
            F.sha2(F.concat(cpf_digitos, F.lit(CPF_PEPPER)), 256).alias("cpf_token"),
            (F.length(F.regexp_replace("cpf", r"\D", "")).between(1, 11)).alias("cpf_formato_ok"),
            F.expr("try_cast(score_mercado AS INT)").alias("score_mercado"),
            F.trim("faixa_renda_estimada").alias("faixa_renda_estimada"),
            propensao.alias("propensao_credito"),
            F.to_date("data_referencia").alias("data_referencia"),
            F.col("_ingerido_em").alias("disponivel_em"),
            "_arquivo",
            "_rescued_data",
        )
        # duplicatas exatas dentro do mesmo arquivo/reenvio; versões corrigidas são mantidas
        .withWatermark("disponivel_em", "3 days")
        .dropDuplicatesWithinWatermark(
            ["cpf_token", "data_referencia", "score_mercado", "faixa_renda_estimada", "propensao_credito"]
        )
    )
