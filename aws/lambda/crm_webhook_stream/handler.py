"""Webhook Data Stream do CRM: API Gateway -> esta Lambda -> Kinesis (realtime) / Firehose (batch).

Contrato com o CRM: responder 2xx só depois de gravar. Em falha devolvemos 5xx e o CRM
reenvia (at-least-once) -> duplicatas são esperadas e tratadas no Delta por event_id.
"""

import base64
import json
import os
from datetime import datetime, timezone

import boto3

from crm_eventos import (
    chave_particao_kinesis,
    classificar,
    derivar_event_id,
    eh_tipo_conhecido,
    verificar_assinatura,
)

STREAM_REALTIME = os.environ["KINESIS_STREAM_REALTIME"]
FIREHOSE_BATCH = os.environ["FIREHOSE_STREAM_BATCH"]
SEGREDO_ARN = os.environ["WEBHOOK_SECRET_ARN"]
CONFIG_PATH = os.environ.get("CRITICIDADE_CONFIG_PATH", "criticidade_eventos.json")

kinesis = boto3.client("kinesis")
firehose = boto3.client("firehose")
cloudwatch = boto3.client("cloudwatch")
_segredo = boto3.client("secretsmanager").get_secret_value(SecretId=SEGREDO_ARN)["SecretString"].encode()

with open(CONFIG_PATH, encoding="utf-8") as f:
    # Em produção vem do AppConfig (extension layer), recarregado sem deploy
    CONFIG = json.load(f)


def _corpo(event) -> bytes:
    corpo = event.get("body") or ""
    return base64.b64decode(corpo) if event.get("isBase64Encoded") else corpo.encode()


def _enviar_kinesis(registros: list[dict]) -> None:
    pendentes = registros
    for _ in range(3):  # retenta só os registros que falharam (PutRecords é parcial)
        resp = kinesis.put_records(StreamName=STREAM_REALTIME, Records=pendentes)
        if resp["FailedRecordCount"] == 0:
            return
        pendentes = [r for r, res in zip(pendentes, resp["Records"]) if "ErrorCode" in res]
    raise RuntimeError(f"{len(pendentes)} registros não gravados no Kinesis")


def _enviar_firehose(registros: list[dict]) -> None:
    resp = firehose.put_record_batch(DeliveryStreamName=FIREHOSE_BATCH, Records=registros)
    if resp["FailedPutCount"]:
        raise RuntimeError(f"{resp['FailedPutCount']} registros não gravados no Firehose")


def lambda_handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    corpo = _corpo(event)
    if not verificar_assinatura(corpo, headers.get("x-crm-signature"), headers.get("x-crm-timestamp"), _segredo):
        return {"statusCode": 401, "body": "assinatura inválida"}

    payload = json.loads(corpo)
    eventos = payload if isinstance(payload, list) else payload.get("events", [payload])
    recebido_em = datetime.now(timezone.utc).isoformat()

    realtime, batch, desconhecidos = [], [], 0
    for ev in eventos:
        ev["event_id"] = derivar_event_id(ev)
        ev["_recebido_em"] = recebido_em
        ev["_origem"] = "webhook"
        dado = json.dumps(ev, ensure_ascii=False).encode("utf-8")
        if classificar(ev, CONFIG) == "realtime":
            realtime.append({"Data": dado, "PartitionKey": chave_particao_kinesis(ev)})
        else:
            batch.append({"Data": dado + b"\n"})
            desconhecidos += 0 if eh_tipo_conhecido(ev, CONFIG) else 1

    try:
        for i in range(0, len(realtime), 500):  # limite do PutRecords
            _enviar_kinesis(realtime[i:i + 500])
        for i in range(0, len(batch), 500):     # limite do PutRecordBatch
            _enviar_firehose(batch[i:i + 500])
    except Exception:
        # 5xx: o CRM reenvia o lote inteiro; o que já foi gravado vira duplicata e o Delta deduplica
        return {"statusCode": 503, "body": "falha ao gravar, reenviar"}

    cloudwatch.put_metric_data(
        Namespace="FinVarejo/CRM",
        MetricData=[
            {"MetricName": "EventosRealtime", "Value": len(realtime), "Unit": "Count"},
            {"MetricName": "EventosBatch", "Value": len(batch), "Unit": "Count"},
            {"MetricName": "EventoTipoDesconhecido", "Value": desconhecidos, "Unit": "Count"},
        ],
    )
    return {"statusCode": 200, "body": json.dumps({"recebidos": len(eventos)})}
