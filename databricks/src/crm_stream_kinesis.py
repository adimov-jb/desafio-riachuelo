# Databricks notebook source
# Webhook (Data Stream) em tempo real: Kinesis Data Streams -> Bronze -> Silver (mesmo MERGE do batch).
#
# Entrega at-least-once em toda a cadeia (CRM reenvia, Lambda retenta, Kinesis pode
# repetir na leitura) -> a deduplicação é responsabilidade da Silver, por event_id:
#  1. dentro do micro-batch: row_number por event_id (merge_silver)
#  2. entre micro-batches e contra o export diário: MERGE por (event_date, event_id)
#  3. Bronze com escrita idempotente (txnAppId/txnVersion): reprocessar um micro-batch
#     após falha não duplica a Bronze
#
# O motor antifraude NÃO depende deste job: lê o mesmo stream via enhanced fan-out com
# latência de ~200 ms. Este job alimenta o lakehouse (latência alvo p95 < 2 min).

from pyspark.sql import functions as F

from crm_comum import criar_silver, merge_silver, padronizar

dbutils.widgets.text("catalog", "finvarejo_prod")
CATALOGO = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOGO}.crm.bronze_crm_webhook"
CHECKPOINT = f"/Volumes/{CATALOGO}/crm/checkpoints/crm_webhook_silver"

criar_silver(spark, CATALOGO)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE} (
      payload STRING, partition_key STRING, sequence_number STRING, shard_id STRING,
      chegada_kinesis TIMESTAMP, _ingerido_em TIMESTAMP, dia_ingestao DATE
    )
    PARTITIONED BY (dia_ingestao)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'contem_pii' = 'true')
""")

# COMMAND ----------

stream = (
    spark.readStream.format("kinesis")
    .option("streamName", "crm-eventos-realtime")
    .option("region", "sa-east-1")
    .option("serviceCredential", "finvarejo-kinesis-leitura")  # UC service credential (IAM role)
    .option("initialPosition", "trim_horizon")
    .load()
    .select(
        F.col("data").cast("string").alias("payload"),
        F.col("partitionKey").alias("partition_key"),
        F.col("sequenceNumber").alias("sequence_number"),
        F.col("shardId").alias("shard_id"),
        F.col("approximateArrivalTimestamp").alias("chegada_kinesis"),
        F.current_timestamp().alias("_ingerido_em"),
    )
    .withColumn("dia_ingestao", F.to_date("_ingerido_em"))
)


def processar_lote(lote, batch_id: int) -> None:
    lote.persist()
    (
        lote.write.format("delta").mode("append")
        .option("txnAppId", "crm_webhook_bronze").option("txnVersion", batch_id)
        .saveAsTable(BRONZE)
    )
    merge_silver(spark, padronizar(lote, origem="webhook"), CATALOGO)
    lote.unpersist()


(
    stream.writeStream
    .foreachBatch(processar_lote)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .queryName("crm_webhook_silver")
    .start()
)
