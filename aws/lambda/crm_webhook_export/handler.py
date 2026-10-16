"""Webhook de "export pronto" do Export Raw User Data.

O CRM chama este endpoint com o(s) link(s) S3 do export. A Lambda só valida e registra
na tabela de controle (DynamoDB); quem copia é o MWAA, que está aguardando esse estado.
Assim o webhook responde em milissegundos e uma notificação repetida não dispara
nada em dobro (update condicional).
"""

import base64
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from crm_eventos import verificar_assinatura

TABELA_CONTROLE = os.environ["TABELA_CONTROLE"]
SEGREDO_ARN = os.environ["WEBHOOK_SECRET_ARN"]
VALIDADE_LINK_H = 24

controle = boto3.resource("dynamodb").Table(TABELA_CONTROLE)
cloudwatch = boto3.client("cloudwatch")
_segredo = boto3.client("secretsmanager").get_secret_value(SecretId=SEGREDO_ARN)["SecretString"].encode()


def lambda_handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    corpo = event.get("body") or ""
    corpo = base64.b64decode(corpo) if event.get("isBase64Encoded") else corpo.encode()
    if not verificar_assinatura(corpo, headers.get("x-crm-signature"), headers.get("x-crm-timestamp"), _segredo):
        return {"statusCode": 401, "body": "assinatura inválida"}

    aviso = json.loads(corpo)
    request_id = aviso["request_id"]
    agora = datetime.now(timezone.utc)
    expira_em = aviso.get("expires_at") or (agora + timedelta(hours=VALIDADE_LINK_H)).isoformat()

    try:
        # Só transita REQUESTED -> READY para o request_id que NÓS pedimos: ignora
        # notificação duplicada, atrasada ou de um request desconhecido.
        item = controle.update_item(
            Key={"export_date": aviso["export_date"]},
            UpdateExpression="SET #s = :ready, links = :links, manifesto = :man, "
                             "link_expira_em = :exp, webhook_recebido_em = :agora",
            ConditionExpression="#s = :requested AND request_id = :rid",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":ready": "READY", ":requested": "REQUESTED", ":rid": request_id,
                ":links": aviso["files"], ":man": aviso.get("manifest"),
                ":exp": expira_em, ":agora": agora.isoformat(),
            },
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"statusCode": 200, "body": "já registrado ou request desconhecido"}
        raise

    solicitado_em = datetime.fromisoformat(item["solicitado_em"])
    cloudwatch.put_metric_data(
        Namespace="FinVarejo/CRM",
        MetricData=[{"MetricName": "ExportLatenciaWebhookMin",
                     "Value": (agora - solicitado_em).total_seconds() / 60, "Unit": "None"}],
    )
    return {"statusCode": 200, "body": "ok"}
