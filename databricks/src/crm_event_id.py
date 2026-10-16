"""Derivação do event_id no lakehouse. DEVE ser idêntica a aws/lambda/comum/crm_eventos.py
(o mesmo evento chega pelo webhook e pelo export e precisa da mesma chave).
A paridade é garantida por aws/tests/test_paridade_event_id.py.
"""

import hashlib
import json


def derivar_event_id(evento: dict) -> str:
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
