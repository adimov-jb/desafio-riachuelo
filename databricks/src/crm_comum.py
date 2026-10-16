"""Transformação e MERGE de deduplicação compartilhados pelo batch (export) e pelo streaming (webhook).

Um único código grava a Silver: os dois pipelines produzem o mesmo schema, a mesma
chave (event_id) e a mesma regra de prioridade, então não existem duas "verdades".
"""

import json

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from crm_event_id import derivar_event_id

SILVER = "silver_crm_eventos"

ESQUEMA_EVENTO = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("properties", T.MapType(T.StringType(), T.StringType())),
])

# Prioridade quando o mesmo evento chega pelas duas origens: o export é a fonte
# autoritativa (completo, reconciliado pelo CRM); o webhook é provisório (chega antes).
PRIORIDADE = {"export": 0, "webhook": 1}


@F.pandas_udf(T.StringType())
def _event_id_udf(payload):
    # só é chamado quando o CRM não manda event_id nativo
    return payload.map(lambda p: derivar_event_id(json.loads(p)))


def padronizar(df_payload: DataFrame, origem: str) -> DataFrame:
    """df_payload: coluna `payload` (JSON string do evento) + `_ingerido_em`."""
    ev = df_payload.withColumn("e", F.from_json("payload", ESQUEMA_EVENTO))
    return ev.select(
        F.coalesce(F.col("e.event_id"), _event_id_udf("payload")).alias("event_id"),
        F.col("e.user_id").alias("user_id"),
        F.col("e.event_type").alias("event_type"),
        F.to_timestamp("e.event_ts").alias("event_ts"),
        F.to_date(F.to_timestamp("e.event_ts")).alias("event_date"),  # UTC nas duas origens
        F.col("e.properties").alias("properties"),
        "payload",
        F.lit(origem).alias("_origem"),
        "_ingerido_em",
    )


def criar_silver(spark: SparkSession, catalogo: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalogo}.crm.{SILVER} (
          event_id STRING NOT NULL, user_id STRING, event_type STRING,
          event_ts TIMESTAMP, event_date DATE, properties MAP<STRING, STRING>, payload STRING,
          _origem STRING, _primeira_origem STRING, _ingerido_em TIMESTAMP,
          _visto_webhook_em TIMESTAMP, _atualizado_em TIMESTAMP
        )
        CLUSTER BY (event_date, user_id)
        TBLPROPERTIES (
          'delta.enableChangeDataFeed' = 'true',
          'delta.autoOptimize.optimizeWrite' = 'true',
          'delta.autoOptimize.autoCompact' = 'true',
          'delta.deletedFileRetentionDuration' = 'interval 7 days',
          'contem_pii' = 'true'
        )
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalogo: str) -> None:
    """Upsert idempotente por event_id.

    1. Dentro do lote: 1 linha por event_id (export vence webhook; depois o mais recente).
    2. Contra a tabela: casa por (event_date, event_id). event_date no ON permite pular
       arquivos (clustering) em vez de varrer a Silver inteira a cada micro-batch.
    3. Evento novo -> INSERT. Já existe vindo do webhook e agora chegou pelo export ->
       UPDATE com a versão autoritativa, preservando quando o webhook o viu (reconciliação).
       Qualquer outra repetição (retentativa do CRM, reprocesso) não altera nada.
    """
    prioridade = F.create_map(*[F.lit(x) for kv in PRIORIDADE.items() for x in kv])
    janela = Window.partitionBy("event_id").orderBy(prioridade[F.col("_origem")], F.col("_ingerido_em").desc())
    lote = (
        df.filter(F.col("event_id").isNotNull() & F.col("event_date").isNotNull())
        .withColumn("_rn", F.row_number().over(janela))
        .filter("_rn = 1")
        .drop("_rn")
    )
    datas = [r.event_date.isoformat() for r in lote.select("event_date").distinct().collect()]
    if not datas:
        return
    lote.createOrReplaceTempView("lote_crm")
    filtro_datas = ", ".join(f"DATE'{d}'" for d in datas)
    spark.sql(f"""
        MERGE INTO {catalogo}.crm.{SILVER} t
        USING lote_crm s
        ON t.event_date IN ({filtro_datas})
           AND t.event_date = s.event_date
           AND t.event_id = s.event_id
        WHEN MATCHED AND s._origem = 'export' AND t._origem = 'webhook' THEN UPDATE SET
          user_id = s.user_id, event_type = s.event_type, event_ts = s.event_ts,
          properties = s.properties, payload = s.payload, _origem = s._origem,
          _ingerido_em = s._ingerido_em, _atualizado_em = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          event_id, user_id, event_type, event_ts, event_date, properties, payload,
          _origem, _primeira_origem, _ingerido_em, _visto_webhook_em, _atualizado_em
        ) VALUES (
          s.event_id, s.user_id, s.event_type, s.event_ts, s.event_date, s.properties, s.payload,
          s._origem, s._origem, s._ingerido_em,
          CASE WHEN s._origem = 'webhook' THEN s._ingerido_em END, current_timestamp()
        )
    """)
