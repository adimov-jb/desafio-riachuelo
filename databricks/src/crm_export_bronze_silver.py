# Databricks notebook source
# Export Raw User Data (até 2 TB/dia): raw S3 -> Bronze Delta -> Silver (MERGE dedup).
# Disparado pelo MWAA depois que a cópia foi validada (_SUCCESS no prefixo do dia).
#
# Formato e layout (decisões para 2 TB):
#  - Raw S3 : arquivos como o CRM entregou (JSON Lines .gz), prefixo export_date=AAAA-MM-DD/.
#             Imutável; é a prova do que foi recebido. Lifecycle: Glacier IR em 30 dias.
#  - Bronze : Delta/Parquet com ZSTD, PARTITIONED BY (export_date). 1 partição por dia
#             (~300-400 GB comprimidos a partir de 2 TB de JSON) = partições grandes, sem
#             small files. replaceWhere na partição torna a carga idempotente.
#  - Silver : Delta com liquid clustering (event_date, user_id): MERGE por event_id com
#             data skipping, sem o skew de partição Hive por tipo de evento.
#  - Arquivos-alvo de ~512 MB (optimizeWrite/autoCompact) para leitura eficiente.
#
# gzip não é divisível: um .json.gz de 50 GB vira 1 task de horas. O contrato com o CRM
# pede arquivos de no máximo ~1 GB; se vierem maiores, o passo de cópia os re-particiona.

from pyspark.sql import functions as F

from crm_comum import criar_silver, merge_silver, padronizar

dbutils.widgets.text("export_date", "")
dbutils.widgets.text("catalog", "finvarejo_prod")
EXPORT_DATE = dbutils.widgets.get("export_date")
CATALOGO = dbutils.widgets.get("catalog")
ORIGEM = f"s3://finvarejo-crm-raw/export_raw/export_date={EXPORT_DATE}/"
BRONZE = f"{CATALOGO}.crm.bronze_crm_export"

spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
spark.conf.set("spark.sql.files.maxPartitionBytes", str(512 * 1024 * 1024))

try:
    dbutils.fs.ls(ORIGEM + "_SUCCESS")
except Exception as erro:
    raise RuntimeError(f"Cópia de {EXPORT_DATE} não validada (_SUCCESS ausente)") from erro

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE} (
      payload STRING, record_type STRING, _arquivo STRING,
      _ingerido_em TIMESTAMP, export_date DATE
    )
    PARTITIONED BY (export_date)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'contem_pii' = 'true')
""")

bronze = (
    spark.read.text(ORIGEM + "*.json*")  # payload intacto: schema do CRM muda sem quebrar a carga
    .select(
        F.col("value").alias("payload"),
        F.get_json_object("value", "$.record_type").alias("record_type"),  # event | user_attributes
        F.col("_metadata.file_path").alias("_arquivo"),
        F.current_timestamp().alias("_ingerido_em"),
        F.lit(EXPORT_DATE).cast("date").alias("export_date"),
    )
)
(
    bronze.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"export_date = DATE'{EXPORT_DATE}'")   # reexecução substitui só o dia
    .saveAsTable(BRONZE)
)

# COMMAND ----------

criar_silver(spark, CATALOGO)
eventos = padronizar(
    spark.table(BRONZE).filter(f"export_date = DATE'{EXPORT_DATE}' AND record_type = 'event'"),
    origem="export",
)
merge_silver(spark, eventos, CATALOGO)
# Atributos de usuário (record_type = 'user_attributes') seguem para uma Silver SCD2
# própria (silver_crm_atributos_usuario), no mesmo padrão; omitido aqui por brevidade.

# COMMAND ----------

# Reconciliação stream x batch: dos eventos críticos do dia, quantos o webhook já tinha
# entregado? Queda dessa taxa = perda no realtime (endpoint fora, throttling no Kinesis).
spark.sql(f"""
    INSERT INTO {CATALOGO}.ops.crm_reconciliacao
    SELECT DATE'{EXPORT_DATE}' AS export_date, event_type,
           count(*) AS eventos_export,
           count_if(_visto_webhook_em IS NOT NULL) AS vistos_no_webhook,
           round(100 * count_if(_visto_webhook_em IS NOT NULL) / count(*), 2) AS pct_cobertura_webhook,
           current_timestamp() AS calculado_em
    FROM {CATALOGO}.crm.silver_crm_eventos
    WHERE event_date = DATE'{EXPORT_DATE}'
      AND event_type IN ('pagamento', 'compra', 'alteracao_cadastral', 'login', 'opt_out', 'consentimento')
    GROUP BY event_type
""")
