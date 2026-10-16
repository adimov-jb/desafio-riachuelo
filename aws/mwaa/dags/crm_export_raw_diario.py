"""DAG diária do Export Raw User Data do CRM (MWAA, Airflow 2.10).

Restrições do CRM: 1 requisição por dia (janela UTC), link expira em 24h, até 2 TB.

Fluxo (estado durável na tabela DynamoDB crm_export_controle, chave export_date):
  reservar_requisicao -> solicitar_export -> aguardar_webhook -> verificar_prazo_link
  -> copiar_para_raw (Glue) -> validar_copia -> carregar_lakehouse (Databricks) -> concluir

Idempotência: a DAG pode ser reexecutada a qualquer momento. Cada passo lê o estado
e pula o que já foi feito; a requisição ao CRM é protegida por escrita condicional e
por Idempotency-Key, então uma reexecução nunca consome a cota do dia duas vezes.
"""

from datetime import datetime, timedelta, timezone

import boto3
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.python import PythonSensor

REGIAO = "sa-east-1"
TABELA_CONTROLE = "crm_export_controle"
BUCKET_RAW = "finvarejo-crm-raw"
SNS_ALERTA = Variable.get("sns_alerta_dados_arn", default_var="")
ESTADOS_JA_SOLICITADO = {"REQUESTED", "READY", "COPIED", "LOADED"}

dynamo = boto3.resource("dynamodb", region_name=REGIAO).Table(TABELA_CONTROLE)


def _alertar(contexto):
    if SNS_ALERTA:
        ti = contexto["task_instance"]
        boto3.client("sns", region_name=REGIAO).publish(
            TopicArn=SNS_ALERTA,
            Subject=f"[CRM export] falha em {ti.task_id}",
            Message=f"DAG {ti.dag_id}, export_date {contexto['ds']}, tentativa {ti.try_number}: {contexto.get('exception')}",
        )


def _estado(export_date: str) -> dict:
    return dynamo.get_item(Key={"export_date": export_date}, ConsistentRead=True).get("Item", {})


def _atualizar(export_date: str, **campos) -> None:
    nomes = {f"#{k}": k for k in campos}
    valores = {f":{k}": v for k, v in campos.items()}
    dynamo.update_item(
        Key={"export_date": export_date},
        UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in campos),
        ExpressionAttributeNames=nomes,
        ExpressionAttributeValues=valores,
    )


@dag(
    dag_id="crm_export_raw_diario",
    # 00:15 UTC: logo após a virada da janela UTC -> ~23h de folga para falhas e reprocesso
    schedule="15 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia-dados",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": _alertar,
    },
    tags=["crm", "batch", "lgpd"],
    doc_md=__doc__,
)
def crm_export_raw_diario():

    @task
    def reservar_requisicao(ds=None) -> str:
        """Cria a linha do dia só se não existir (escrita condicional = trava distribuída)."""
        try:
            dynamo.put_item(
                Item={"export_date": ds, "status": "RESERVED", "reservado_em": datetime.now(timezone.utc).isoformat()},
                ConditionExpression="attribute_not_exists(export_date)",
            )
        except dynamo.meta.client.exceptions.ConditionalCheckFailedException:
            pass  # reexecução: o estado existente decide o que fazer
        return ds

    @task
    def solicitar_export(export_date: str) -> None:
        estado = _estado(export_date)
        if estado.get("status") in ESTADOS_JA_SOLICITADO:
            raise AirflowSkipException(f"Export de {export_date} já solicitado ({estado['status']})")

        crm = Variable.get("crm_api", deserialize_json=True)  # Secrets Manager backend
        resp = requests.post(
            f"{crm['base_url']}/users/export/raw",
            headers={
                "Authorization": f"Bearer {crm['token']}",
                # mesma chave em toda retentativa: o CRM devolve o mesmo request_id
                "Idempotency-Key": f"finvarejo-export-raw-{export_date}",
            },
            json={"start_date": export_date, "end_date": export_date, "callback": crm["callback_url"]},
            timeout=60,
        )
        if resp.status_code == 429:
            # cota do dia consumida e nós não temos request_id: não há como recuperar hoje
            _atualizar(export_date, status="QUOTA_EXHAUSTED")
            raise AirflowFailException("Cota diária do CRM já consumida sem request registrado: acionar plantão")
        resp.raise_for_status()  # 5xx/timeout -> retry do Airflow (seguro pela Idempotency-Key)
        _atualizar(export_date, status="REQUESTED", request_id=resp.json()["request_id"],
                   solicitado_em=datetime.now(timezone.utc).isoformat())

    def _export_pronto(export_date: str) -> bool:
        return _estado(export_date).get("status") in {"READY", "COPIED", "LOADED"}

    aguardar_webhook = PythonSensor(
        task_id="aguardar_webhook",
        python_callable=_export_pronto,
        op_args=["{{ ds }}"],
        mode="reschedule",            # não prende slot de worker durante horas
        poke_interval=300,
        timeout=int(timedelta(hours=18).total_seconds()),
        trigger_rule="none_failed",   # roda também quando solicitar_export foi pulado
    )

    @task(trigger_rule="none_failed")
    def verificar_prazo_link(export_date: str) -> None:
        estado = _estado(export_date)
        if estado["status"] in {"COPIED", "LOADED"}:
            return
        restante = datetime.fromisoformat(estado["link_expira_em"]) - datetime.now(timezone.utc)
        boto3.client("cloudwatch", region_name=REGIAO).put_metric_data(
            Namespace="FinVarejo/CRM",
            MetricData=[{"MetricName": "ExportLinkHorasRestantes", "Value": restante.total_seconds() / 3600}],
        )
        if restante < timedelta(hours=3):
            raise AirflowFailException(f"Link expira em {restante}; cópia de até 2 TB não cabe com segurança")

    copiar_para_raw = GlueJobOperator(
        task_id="copiar_para_raw",
        job_name="crm_export_copia_raw",
        script_args={"--export_date": "{{ ds }}", "--tabela_controle": TABELA_CONTROLE, "--bucket_raw": BUCKET_RAW},
        region_name=REGIAO,
        wait_for_completion=True,
        deferrable=True,
        retries=2,  # o job Glue retoma: pula arquivos já copiados e íntegros
    )

    @task
    def validar_copia(export_date: str) -> None:
        estado = _estado(export_date)
        if estado["status"] in {"COPIED", "LOADED"}:
            return
        esperados = {f["nome"]: int(f["tamanho_bytes"]) for f in estado["links"]}
        prefixo = f"export_raw/export_date={export_date}/"
        s3 = boto3.client("s3", region_name=REGIAO)
        encontrados = {}
        for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET_RAW, Prefix=prefixo):
            for obj in pagina.get("Contents", []):
                encontrados[obj["Key"].removeprefix(prefixo)] = obj["Size"]
        divergentes = {n: (t, encontrados.get(n)) for n, t in esperados.items() if encontrados.get(n) != t}
        if divergentes:
            raise AirflowFailException(f"Cópia divergente do manifesto: {list(divergentes)[:10]}")
        s3.put_object(Bucket=BUCKET_RAW, Key=prefixo + "_SUCCESS", Body=b"")
        _atualizar(export_date, status="COPIED", bytes_copiados=sum(esperados.values()),
                   copiado_em=datetime.now(timezone.utc).isoformat())

    carregar_lakehouse = DatabricksRunNowOperator(
        task_id="carregar_lakehouse",
        databricks_conn_id="databricks_default",
        job_name="crm_export_bronze_silver",
        job_parameters={"export_date": "{{ ds }}"},
        deferrable=True,
    )

    @task
    def concluir(export_date: str) -> None:
        estado = _estado(export_date)
        fim = datetime.now(timezone.utc)
        _atualizar(export_date, status="LOADED", carregado_em=fim.isoformat())
        inicio = datetime.fromisoformat(estado["reservado_em"])
        boto3.client("cloudwatch", region_name=REGIAO).put_metric_data(
            Namespace="FinVarejo/CRM",
            MetricData=[
                {"MetricName": "ExportConcluido", "Value": 1, "Unit": "Count"},
                {"MetricName": "ExportDuracaoTotalHoras", "Value": (fim - inicio).total_seconds() / 3600},
                {"MetricName": "ExportGB", "Value": float(estado.get("bytes_copiados", 0)) / 1e9},
            ],
        )

    data = reservar_requisicao()
    solicitado = solicitar_export(data)
    prazo = verificar_prazo_link(data)
    validado = validar_copia(data)
    solicitado >> aguardar_webhook >> prazo >> copiar_para_raw >> validado >> carregar_lakehouse >> concluir(data)


crm_export_raw_diario()
