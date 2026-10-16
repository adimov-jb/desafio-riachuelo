# Databricks notebook source
# Lakehouse Monitoring (data quality monitoring) da Silver de eventos do CRM.
# Executado uma vez (idempotente) pelo deploy do bundle. Gera tabelas de perfil e de
# drift no schema ops; os alertas SQL abaixo leem essas tabelas.

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    MonitorCronSchedule,
    MonitorDestination,
    MonitorMetric,
    MonitorMetricType,
    MonitorNotifications,
    MonitorTimeSeries,
)
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "finvarejo_prod")
CATALOGO = dbutils.widgets.get("catalog")
w = WorkspaceClient()

metrica_double = T.StructField("valor", T.DoubleType()).json()

try:
    w.quality_monitors.create(
        table_name=f"{CATALOGO}.crm.silver_crm_eventos",
        assets_dir=f"/Workspace/Shared/monitoramento/{CATALOGO}/crm",
        output_schema_name=f"{CATALOGO}.ops",
        time_series=MonitorTimeSeries(timestamp_col="event_ts", granularities=["1 hour", "1 day"]),
        # perfil e drift por tipo de evento e por origem (webhook x export)
        slicing_exprs=["event_type", "_origem"],
        custom_metrics=[
            MonitorMetric(
                type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
                name="pct_sem_user_id",
                input_columns=[":table"],
                definition="avg(CASE WHEN user_id IS NULL THEN 100.0 ELSE 0 END)",
                output_data_type=metrica_double,
            ),
            MonitorMetric(
                type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
                name="atraso_medio_ingestao_min",
                input_columns=[":table"],
                definition="avg((unix_timestamp(_ingerido_em) - unix_timestamp(event_ts)) / 60)",
                output_data_type=metrica_double,
            ),
        ],
        # após a carga batch (SLA 12:00 UTC); o realtime é vigiado pelos alertas de freshness
        schedule=MonitorCronSchedule(quartz_cron_expression="0 30 12 * * ?", timezone_id="UTC"),
        notifications=MonitorNotifications(
            on_failure=MonitorDestination(email_addresses=["engenharia.dados@finvarejo.example"])
        ),
    )
except ResourceAlreadyExists:
    pass

# COMMAND ----------

# Alertas Databricks SQL (criados como queries agendadas com alerta; condição: retorna linhas)
ALERTAS = {
    # SLA batch: carga do dia anterior não chegou até 12:00 UTC
    "crm_batch_atrasado": f"""
        SELECT max(export_date) AS ultima_carga FROM {CATALOGO}.crm.bronze_crm_export
        HAVING max(export_date) < date_sub(current_date(), 1)""",
    # Freshness realtime: nenhum evento crítico do webhook há mais de 15 min
    "crm_realtime_parado": f"""
        SELECT max(_ingerido_em) AS ultimo FROM {CATALOGO}.crm.silver_crm_eventos
        WHERE _primeira_origem = 'webhook' AND event_date >= date_sub(current_date(), 1)
        HAVING max(_ingerido_em) < current_timestamp() - INTERVAL 15 MINUTES""",
    # Volume: dia com menos de 70% da média dos 7 dias anteriores, por tipo de evento
    "crm_volume_anomalo": f"""
        WITH d AS (SELECT event_date, event_type, count(*) n FROM {CATALOGO}.crm.silver_crm_eventos
                   WHERE event_date >= date_sub(current_date(), 9) GROUP BY ALL)
        SELECT h.event_type, h.n, avg(p.n) media_7d FROM d h JOIN d p
          ON p.event_type = h.event_type AND p.event_date BETWEEN date_sub(h.event_date, 7) AND date_sub(h.event_date, 1)
        WHERE h.event_date = date_sub(current_date(), 1) GROUP BY h.event_type, h.n HAVING h.n < 0.7 * avg(p.n)""",
    # Drift: distância de distribuição alta em colunas da Silver (tabela gerada pelo monitor)
    "crm_drift": f"""
        SELECT window, column_name, slice_key, slice_value, js_distance
        FROM {CATALOGO}.ops.silver_crm_eventos_drift_metrics
        WHERE window.start >= current_date() - INTERVAL 1 DAY AND js_distance > 0.2""",
    # Reconciliação: webhook entregou menos de 99% dos eventos críticos que o export trouxe
    "crm_webhook_perdendo_eventos": f"""
        SELECT * FROM {CATALOGO}.ops.crm_reconciliacao
        WHERE export_date = date_sub(current_date(), 1) AND pct_cobertura_webhook < 99""",
}
for nome, sql in ALERTAS.items():
    print(f"-- {nome}\n{sql}\n")
