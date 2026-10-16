# Databricks notebook source
# Gold: feature table point-in-time de cliente enriquecido com Serasa.
#
# Chave: (id_cliente, ts_feature TIMESERIES). ts_feature = momento em que a informação
# ficou DISPONÍVEL para a FinVarejo (chegada do arquivo), não a data_referencia do Serasa:
# um arquivo com data_referencia D que chega em D+1 06:00 não pode alimentar uma
# decisão tomada em D 18:00. Atributos internos (score_interno, canal, UF) vêm do
# histórico SCD2 do cliente na mesma data -> nenhuma feature "olha o futuro".
#
# Tabela Delta EXTERNA em S3 (para registro no Glue/Athena) e feature table no Unity
# Catalog (qualquer tabela Delta com PRIMARY KEY no UC é feature table).

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "finvarejo_dev")
dbutils.widgets.text("gold_path", "s3://finvarejo-lakehouse-dev/gold/")
CATALOG = dbutils.widgets.get("catalog")
GOLD_PATH = dbutils.widgets.get("gold_path").rstrip("/") + "/features_cliente_serasa"
TABELA = f"{CATALOG}.gold.features_cliente_serasa"

FAIXA_ORDINAL = {"Ate 2 SM": 1, "2 a 5 SM": 2, "5 a 10 SM": 3, "10 a 20 SM": 4, "Acima de 20 SM": 5}
PROPENSAO_ORDINAL = {"Baixa": 0, "Media": 1, "Alta": 2}

# COMMAND ----------

serasa = spark.table(f"{CATALOG}.serasa.silver_serasa_score")

# Histórico SCD2 do cliente replicado do FinVarejo via CDC (silver do domínio core),
# já com cpf_token gerado com o mesmo pepper. Colunas: id_cliente, cpf_token,
# score_interno, canal_aquisicao, estado, regiao, valido_de, valido_ate (null = vigente)
cliente_hist = spark.table(f"{CATALOG}.core.silver_cliente_historico")


def mapa(col, dicionario):
    return F.create_map(*[F.lit(x) for kv in dicionario.items() for x in kv])[F.col(col)]


# Variação do score de mercado vs. observação anterior do mesmo CPF (só olha para trás)
w_cpf = Window.partitionBy("cpf_token").orderBy("disponivel_em")
serasa_feat = serasa.select(
    "cpf_token",
    F.col("disponivel_em").alias("ts_feature"),
    F.col("data_referencia").alias("data_referencia_serasa"),
    "score_mercado",
    mapa("faixa_renda_estimada", FAIXA_ORDINAL).alias("faixa_renda_ordinal"),
    mapa("propensao_credito", PROPENSAO_ORDINAL).alias("propensao_credito_ordinal"),
).withColumn("var_score_mercado_ult_obs", F.col("score_mercado") - F.lag("score_mercado").over(w_cpf))

# As-of join: atributo interno vigente no instante ts_feature
features = (
    serasa_feat.alias("s")
    .join(
        cliente_hist.alias("c"),
        (F.col("s.cpf_token") == F.col("c.cpf_token"))
        & (F.col("c.valido_de") <= F.col("s.ts_feature"))
        & (F.col("c.valido_ate").isNull() | (F.col("s.ts_feature") < F.col("c.valido_ate"))),
        "inner",
    )
    .select(
        "c.id_cliente",
        "s.ts_feature",
        "s.data_referencia_serasa",
        "s.score_mercado",
        "c.score_interno",
        (F.col("s.score_mercado") - F.col("c.score_interno")).alias("dif_score_mercado_interno"),
        "s.faixa_renda_ordinal",
        "s.propensao_credito_ordinal",
        "s.var_score_mercado_ult_obs",
        "c.canal_aquisicao",
        "c.regiao",
        F.current_timestamp().alias("_atualizado_em"),
    )
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA} (
  id_cliente                  BIGINT    NOT NULL COMMENT 'Chave do cliente (sem CPF)',
  ts_feature                  TIMESTAMP NOT NULL COMMENT 'Momento em que a informação ficou disponível',
  data_referencia_serasa      DATE      COMMENT 'Data de referência informada pelo Serasa',
  score_mercado               INT       COMMENT 'Score Serasa 0-1000',
  score_interno               INT       COMMENT 'Score interno vigente em ts_feature',
  dif_score_mercado_interno   INT       COMMENT 'score_mercado - score_interno',
  faixa_renda_ordinal         INT       COMMENT '1 (até 2 SM) a 5 (acima de 20 SM)',
  propensao_credito_ordinal   INT       COMMENT '0 Baixa, 1 Média, 2 Alta',
  var_score_mercado_ult_obs   INT       COMMENT 'Variação vs. observação Serasa anterior',
  canal_aquisicao             STRING,
  regiao                      STRING,
  _atualizado_em              TIMESTAMP,
  CONSTRAINT pk_features_cliente_serasa PRIMARY KEY (id_cliente, ts_feature TIMESERIES)
)
USING DELTA
LOCATION '{GOLD_PATH}'
CLUSTER BY (id_cliente, ts_feature)
COMMENT 'Feature table point-in-time: cliente enriquecido com Serasa. Dono: Engenharia de Dados.'
TBLPROPERTIES ('quality' = 'gold', 'contem_pii' = 'false', 'sla' = 'D0 07:30 BRT')
""")

# Idempotente: reprocessar o mesmo arquivo não duplica (MERGE pela chave)
features.createOrReplaceTempView("features_novas")
spark.sql(f"""
MERGE INTO {TABELA} t
USING features_novas s
ON t.id_cliente = s.id_cliente AND t.ts_feature = s.ts_feature
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Portão de SLA/qualidade: falha o job (e alerta) se a carga do dia não chegou ou encolheu
resumo = spark.sql(f"""
  SELECT max(data_referencia_serasa) AS ultima_ref,
         count_if(to_date(ts_feature) = current_date()) AS linhas_hoje,
         count_if(to_date(ts_feature) = date_sub(current_date(), 1)) AS linhas_ontem
  FROM {TABELA}
""").first()
if resumo.linhas_hoje == 0:
    raise RuntimeError(f"Nenhuma feature publicada hoje; última referência Serasa: {resumo.ultima_ref}")
if resumo.linhas_ontem and resumo.linhas_hoje < 0.9 * resumo.linhas_ontem:
    raise RuntimeError(f"Volume caiu >10% vs. ontem: {resumo.linhas_hoje} vs {resumo.linhas_ontem}")
