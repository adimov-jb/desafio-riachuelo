import hashlib
import hmac
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda" / "comum"))

from crm_eventos import (  # noqa: E402
    chave_particao_kinesis,
    classificar,
    derivar_event_id,
    eh_tipo_conhecido,
    verificar_assinatura,
)

CONFIG = json.loads((Path(__file__).resolve().parents[1] / "config" / "criticidade_eventos.json").read_text("utf-8"))
SEGREDO = b"segredo-teste"


def assinar(corpo: bytes, ts: int) -> str:
    return hmac.new(SEGREDO, f"{ts}.".encode() + corpo, hashlib.sha256).hexdigest()


def test_assinatura_valida():
    corpo = b'{"a":1}'
    assert verificar_assinatura(corpo, assinar(corpo, 1000), "1000", SEGREDO, agora=1100)


def test_assinatura_corpo_adulterado():
    assert not verificar_assinatura(b'{"a":2}', assinar(b'{"a":1}', 1000), "1000", SEGREDO, agora=1100)


def test_assinatura_replay_fora_da_tolerancia():
    corpo = b'{"a":1}'
    assert not verificar_assinatura(corpo, assinar(corpo, 1000), "1000", SEGREDO, agora=1000 + 301)


def test_assinatura_sem_cabecalhos():
    assert not verificar_assinatura(b"{}", None, None, SEGREDO, agora=0)


def test_event_id_nativo_tem_prioridade():
    assert derivar_event_id({"event_id": "abc", "user_id": 1}) == "abc"


def test_event_id_hash_estavel_independente_da_ordem_das_chaves():
    a = {"user_id": 7, "event_type": "compra", "event_ts": "2025-01-10T10:00:00Z", "properties": {"v": 10, "loja": 3}}
    b = {"properties": {"loja": 3, "v": 10}, "event_ts": "2025-01-10T10:00:00Z", "event_type": "compra", "user_id": 7}
    assert derivar_event_id(a) == derivar_event_id(b)
    assert derivar_event_id(a).startswith("h_")


def test_event_id_ignora_metadados_de_recebimento():
    base = {"user_id": 7, "event_type": "compra", "event_ts": "2025-01-10T10:00:00Z"}
    assert derivar_event_id(base) == derivar_event_id({**base, "_recebido_em": "x", "_origem": "webhook"})


def test_event_id_muda_com_o_conteudo():
    base = {"user_id": 7, "event_type": "compra", "event_ts": "2025-01-10T10:00:00Z", "properties": {"v": 10}}
    assert derivar_event_id(base) != derivar_event_id({**base, "properties": {"v": 11}})


def test_classificacao():
    assert classificar({"event_type": "pagamento"}, CONFIG) == "realtime"
    assert classificar({"event_type": "opt_out"}, CONFIG) == "realtime"
    assert classificar({"event_type": "push_aberto"}, CONFIG) == "batch"


def test_tipo_desconhecido_vai_para_batch_e_e_sinalizado():
    ev = {"event_type": "novo_tipo"}
    assert classificar(ev, CONFIG) == "batch"
    assert not eh_tipo_conhecido(ev, CONFIG)


def test_chave_particao_por_cliente():
    assert chave_particao_kinesis({"user_id": 42}) == "42"
    assert chave_particao_kinesis({"event_type": "x"}).startswith("h_")
