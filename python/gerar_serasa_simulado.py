"""Gera arquivos SIMULADOS do Serasa para desenvolvimento e teste do pipeline.

O arquivo real não foi disponibilizado. Os dados aqui são sintéticos (semente fixa)
e reproduzem o layout informado (cpf, score_mercado, faixa_renda_estimada,
propensao_credito, data_referencia) e problemas comuns de um feed externo:
CPF com e sem máscara, CPFs fora da base, linhas duplicadas, score inválido e
variações de grafia na propensão. Nenhuma conclusão de negócio deve ser tirada deles.

Uso: python gerar_serasa_simulado.py <FinVarejo.db> <pasta_saida>
"""

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SEMENTE = 20250110
DATAS = ["2025-01-08", "2025-01-09", "2025-01-10"]
SALARIO_MINIMO = 1518.0
FAIXAS = ["Ate 2 SM", "2 a 5 SM", "5 a 10 SM", "10 a 20 SM", "Acima de 20 SM"]


def faixa_renda(renda: float, rng: np.random.Generator) -> str:
    sm = renda / SALARIO_MINIMO * rng.normal(1.0, 0.15)
    limites = [2, 5, 10, 20]
    return FAIXAS[int(np.searchsorted(limites, sm))]


def propensao(score: float, renda: float, rng: np.random.Generator) -> str:
    z = (score - 550) / 140 + np.log(renda / 5000) * 0.6 + rng.normal(0, 0.7)
    if z > 0.6:
        return "Alta"
    if z > -0.6:
        return "Media"
    return "Baixa"


def main(db_path: str, pasta_saida: str) -> None:
    rng = np.random.default_rng(SEMENTE)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        clientes = pd.read_sql("select cpf, score_interno, renda_declarada from cliente", conn)

    # Score de mercado base por cliente, correlacionado ao interno com ruído
    clientes["score_base"] = np.clip(
        clientes["score_interno"] + rng.normal(30, 120, len(clientes)), 0, 1000
    )
    cpfs_fora_da_base = [f"{n:011d}" for n in rng.integers(10**9, 10**10, 15)]
    saida = Path(pasta_saida)
    saida.mkdir(parents=True, exist_ok=True)

    for i, data in enumerate(DATAS):
        # Cobertura de ~96% da base por dia
        dia = clientes.sample(frac=0.96, random_state=SEMENTE + i).copy()
        dia["score_mercado"] = np.clip(
            np.round(dia["score_base"] + rng.normal(0, 8, len(dia))), 0, 1000
        ).astype(int)
        dia["faixa_renda_estimada"] = [faixa_renda(r, rng) for r in dia["renda_declarada"]]
        dia["propensao_credito"] = [
            propensao(s, r, rng) for s, r in zip(dia["score_mercado"], dia["renda_declarada"])
        ]
        # Serasa envia CPF só com dígitos; ~5% vem com máscara
        digitos = dia["cpf"].str.replace(r"\D", "", regex=True)
        mascarar = rng.random(len(dia)) < 0.05
        dia["cpf"] = np.where(mascarar, dia["cpf"], digitos)
        dia["data_referencia"] = data
        df = dia[["cpf", "score_mercado", "faixa_renda_estimada", "propensao_credito", "data_referencia"]]

        extras = pd.DataFrame({
            "cpf": cpfs_fora_da_base,
            "score_mercado": rng.integers(300, 900, len(cpfs_fora_da_base)),
            "faixa_renda_estimada": rng.choice(FAIXAS, len(cpfs_fora_da_base)),
            "propensao_credito": rng.choice(["Alta", "Media", "Baixa"], len(cpfs_fora_da_base)),
            "data_referencia": data,
        })
        df = pd.concat([df, extras], ignore_index=True).astype({"score_mercado": "object"})

        # Problemas de qualidade típicos de feed externo
        df.loc[df.sample(3, random_state=i).index, "score_mercado"] = ""        # score ausente
        df.loc[df.sample(2, random_state=i + 10).index, "score_mercado"] = 1200  # fora da escala
        df.loc[df.sample(6, random_state=i + 20).index, "propensao_credito"] = "ALTA"
        df.loc[df.sample(6, random_state=i + 30).index, "propensao_credito"] = "Média"
        df = pd.concat([df, df.sample(4, random_state=i + 40)], ignore_index=True)  # duplicadas

        arquivo = saida / f"serasa_score_{data.replace('-', '')}.csv"
        df.sample(frac=1, random_state=i).to_csv(arquivo, sep=";", index=False, encoding="utf-8")
        print(f"{arquivo}: {len(df)} linhas")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
