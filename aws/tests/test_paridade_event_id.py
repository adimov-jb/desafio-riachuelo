"""O event_id derivado pela Lambda (webhook) e pelo Databricks (export) tem de ser igual."""

import importlib.util
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]


def _carregar(nome, caminho):
    spec = importlib.util.spec_from_file_location(nome, caminho)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[nome] = modulo
    spec.loader.exec_module(modulo)
    return modulo


lambda_mod = _carregar("lambda_crm_eventos", RAIZ / "aws" / "lambda" / "comum" / "crm_eventos.py")
databricks_mod = _carregar("databricks_crm_event_id", RAIZ / "databricks" / "src" / "crm_event_id.py")

AMOSTRAS = [
    {"event_id": "nativo-1", "user_id": 1},
    {"user_id": 7, "event_type": "compra", "event_ts": "2025-01-10T10:00:00Z", "properties": {"v": 10.5, "loja": 3}},
    {"user_id": "u-9", "event_type": "opt_out", "event_ts": "2025-01-10T10:00:00.123Z"},
    {"user_id": 7, "event_type": "alteracao_cadastral", "event_ts": "2025-01-10T10:00:00Z",
     "properties": {"campo": "email", "anterior": "joão@x.com", "lista": [1, 2]}},
    {"event_type": "sem_usuario", "properties": None},
]


def test_paridade_lambda_databricks():
    for ev in AMOSTRAS:
        assert lambda_mod.derivar_event_id(ev) == databricks_mod.derivar_event_id(ev), ev
