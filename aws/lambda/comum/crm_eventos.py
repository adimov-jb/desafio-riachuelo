"""Lógica pura compartilhada pelas Lambdas de webhook do CRM (empacotada como Lambda Layer).

Sem dependência de AWS para ser testável localmente (aws/tests).
"""

import hashlib
import hmac
import json
import time

TOLERANCIA_ASSINATURA_S = 300


def verificar_assinatura(corpo: bytes, assinatura_hex: str, timestamp: str, segredo: bytes,
                         agora: float | None = None) -> bool:
    """HMAC-SHA256 de f"{timestamp}.{corpo}" com o segredo do webhook.

    O timestamp no conteúdo assinado + tolerância de 5 min impede replay de uma
    requisição capturada. compare_digest evita ataque de tempo.
    """
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    agora = time.time() if agora is None else agora
    if abs(agora - ts) > TOLERANCIA_ASSINATURA_S:
        return False
    esperado = hmac.new(segredo, f"{ts}.".encode() + corpo, hashlib.sha256).hexdigest()
    return hmac.compare_digest(esperado, assinatura_hex or "")


def derivar_event_id(evento: dict) -> str:
    """Chave de deduplicação estável entre webhook, retentativas e export diário.

    Usa o id nativo do CRM quando existe. Senão, hash do conteúdo canônico
    (user_id, tipo, timestamp do evento e propriedades), que é igual para o mesmo
    evento recebido pelo webhook ou pelo export.
    """
    if evento.get("event_id"):
        return str(evento["event_id"])
    canonico = json.dumps(
        {
            "user_id": evento.get("user_id"),
            "event_type": evento.get("event_type"),
            "event_ts": evento.get("event_ts"),
            "properties": evento.get("properties") or {},
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "h_" + hashlib.sha256(canonico.encode("utf-8")).hexdigest()


def classificar(evento: dict, config: dict) -> str:
    """'realtime' ou 'batch'. Desconhecido -> 'batch' (o export diário garante completude)."""
    return "realtime" if evento.get("event_type") in config.get("realtime", {}) else "batch"


def eh_tipo_conhecido(evento: dict, config: dict) -> bool:
    tipo = evento.get("event_type")
    return tipo in config.get("realtime", {}) or tipo in config.get("batch", {})


def chave_particao_kinesis(evento: dict) -> str:
    """user_id como partition key: ordem por cliente dentro do shard (sequência de eventos
    de um mesmo cliente é o que o antifraude analisa) e distribuição uniforme entre shards."""
    return str(evento.get("user_id") or derivar_event_id(evento))
