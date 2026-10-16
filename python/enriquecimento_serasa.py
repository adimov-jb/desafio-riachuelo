"""Item 1 - Enriquecimento da base de clientes com o score Serasa e análises.

Lê os CSVs diários do Serasa, aplica as regras de qualidade, faz o join com
cliente (FinVarejo.db) via CPF normalizado e gera:
  1. top 10 candidatos a upgrade de limite (score_mercado - score_interno > 0)
  2. top 3 regiões com maior concentração de propensao_credito = "Alta"
  3. boxplot de score_mercado por canal de aquisição na paleta da marca

Pré-requisito: `dbt build` executado (usa a view int_cliente__indicadores_credito
para excluir inadimplentes com a mesma regra do restante da plataforma).

Uso: python enriquecimento_serasa.py <FinVarejo.db> <pasta_csv_serasa> <pasta_saida>
"""

import sqlite3
import sys
import unicodedata
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import brand

COLUNAS_SERASA = ["cpf", "score_mercado", "faixa_renda_estimada", "propensao_credito", "data_referencia"]
PROPENSOES_VALIDAS = {"Alta", "Media", "Baixa"}
ROTULO_CANAL = {"loja_fisica": "Loja física", "app": "App", "site": "Site", "televendas": "Televendas"}


def normalizar_cpf(cpf: pd.Series) -> pd.Series:
    """Mantém só dígitos e completa com zeros à esquerda (Serasa e FinVarejo usam formatos diferentes)."""
    return cpf.astype("string").str.replace(r"\D", "", regex=True).str.zfill(11)


def normalizar_propensao(valor: pd.Series) -> pd.Series:
    sem_acento = valor.astype("string").map(
        lambda v: unicodedata.normalize("NFKD", v).encode("ascii", "ignore").decode() if pd.notna(v) else v
    )
    return sem_acento.str.strip().str.capitalize()


def carregar_serasa(pasta: Path) -> tuple[pd.DataFrame, dict]:
    arquivos = sorted(pasta.glob("serasa_score_*.csv"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum serasa_score_*.csv em {pasta}")
    bruto = pd.concat(
        [pd.read_csv(a, sep=";", dtype=str, encoding="utf-8").assign(_arquivo=a.name) for a in arquivos],
        ignore_index=True,
    )
    faltando = set(COLUNAS_SERASA) - set(bruto.columns)
    if faltando:
        raise ValueError(f"Layout do Serasa mudou, colunas ausentes: {faltando}")

    qualidade = {"arquivos": len(arquivos), "linhas_recebidas": len(bruto)}
    df = bruto.drop_duplicates(subset=COLUNAS_SERASA).copy()
    qualidade["duplicadas_removidas"] = len(bruto) - len(df)

    df["cpf"] = normalizar_cpf(df["cpf"])
    cpf_invalido = df["cpf"].str.len() != 11
    qualidade["cpf_invalido"] = int(cpf_invalido.sum())

    df["score_mercado"] = pd.to_numeric(df["score_mercado"], errors="coerce")
    score_invalido = df["score_mercado"].isna() | ~df["score_mercado"].between(0, 1000)
    qualidade["score_ausente_ou_fora_0_1000"] = int(score_invalido.sum())

    df["propensao_credito"] = normalizar_propensao(df["propensao_credito"])
    qualidade["propensao_fora_do_dominio"] = int((~df["propensao_credito"].isin(PROPENSOES_VALIDAS)).sum())

    df["data_referencia"] = pd.to_datetime(df["data_referencia"], errors="coerce")
    df = df[~cpf_invalido & ~score_invalido & df["data_referencia"].notna()]

    # Visão atual: última data_referencia por CPF (o histórico completo fica na Silver do lakehouse)
    df = df.sort_values("data_referencia").drop_duplicates("cpf", keep="last")
    qualidade["cpfs_validos_visao_atual"] = len(df)
    qualidade["data_referencia_mais_recente"] = df["data_referencia"].max().date().isoformat()
    return df, qualidade


def carregar_clientes(db_path: str) -> pd.DataFrame:
    sql = """
        with regiao_uf as (select distinct estado, regiao from loja)
        select c.id_cliente, c.nome, c.cpf, c.estado, r.regiao, c.canal_aquisicao,
               c.score_interno, i.fl_inadimplente, i.max_dias_atraso_recente
        from cliente c
        left join regiao_uf r on r.estado = c.estado
        left join int_cliente__indicadores_credito i on i.id_cliente = c.id_cliente
    """
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        try:
            clientes = pd.read_sql(sql, conn)
        except pd.errors.DatabaseError as erro:
            raise RuntimeError("Rode `dbt build` antes: int_cliente__indicadores_credito não existe") from erro
    if clientes["regiao"].isna().any():
        raise ValueError("UF sem região mapeada em loja: " + ", ".join(clientes.loc[clientes.regiao.isna(), "estado"].unique()))
    clientes["cpf"] = normalizar_cpf(clientes["cpf"])
    return clientes


def top10_upgrade(base: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    candidatos = base.assign(diferenca=base["score_mercado"] - base["score_interno"])
    candidatos = candidatos[candidatos["diferenca"] > 0]
    ordem = ["diferenca", "score_mercado"]
    sem_filtro = candidatos.nlargest(10, ordem)
    # Upgrade de limite não vai para quem tem contrato inadimplente (regra over90 do dbt)
    # nem para quem teve atraso > 30 dias nos últimos 90 dias (critério de alto risco)
    risco = (candidatos["fl_inadimplente"] == 1) | (candidatos["max_dias_atraso_recente"] > 30)
    elegiveis = candidatos[~risco].nlargest(10, ordem)
    excluidos = int(risco.loc[sem_filtro.index].sum())
    colunas = ["nome", "score_interno", "score_mercado", "diferenca", "canal_aquisicao"]
    return elegiveis[colunas].astype({"score_mercado": int, "diferenca": int}).reset_index(drop=True), excluidos


def top3_regioes_alta(base: pd.DataFrame) -> pd.DataFrame:
    validos = base[base["propensao_credito"].isin(PROPENSOES_VALIDAS)]
    por_regiao = (
        validos.groupby("regiao")
        .agg(clientes=("id_cliente", "size"), clientes_alta=("propensao_credito", lambda s: (s == "Alta").sum()))
        .assign(pct_alta=lambda d: (100 * d["clientes_alta"] / d["clientes"]).round(1))
        .sort_values(["pct_alta", "clientes_alta"], ascending=False)
    )
    return por_regiao.head(3).reset_index()


def grafico_score_por_canal(base: pd.DataFrame, data_ref: str, arquivo: Path) -> None:
    ordem = base.groupby("canal_aquisicao")["score_mercado"].median().sort_values(ascending=False).index.tolist()
    contagem = base["canal_aquisicao"].value_counts()

    sns.set_theme(style="white", rc={
        "axes.facecolor": brand.SUPERFICIE, "figure.facecolor": brand.SUPERFICIE,
        "text.color": brand.TEXTO_PRIMARIO, "axes.labelcolor": brand.TEXTO_SECUNDARIO,
        "xtick.color": brand.TEXTO_SECUNDARIO, "ytick.color": brand.TEXTO_SECUNDARIO,
        "font.family": "DejaVu Sans",
    })
    fig, ax = plt.subplots(figsize=(9, 5.5))
    sns.boxplot(
        data=base, x="canal_aquisicao", y="score_mercado", order=ordem, hue="canal_aquisicao",
        palette=brand.CORES_CANAL, legend=False, width=0.5, linewidth=1.2, showfliers=False,
        boxprops={"alpha": 0.85}, medianprops={"color": brand.SUPERFICIE, "linewidth": 2}, ax=ax,
    )
    sns.stripplot(
        data=base, x="canal_aquisicao", y="score_mercado", order=ordem, color=brand.TEXTO_PRIMARIO,
        size=2.5, alpha=0.25, jitter=0.18, ax=ax,
    )
    medianas = base.groupby("canal_aquisicao")["score_mercado"].median()
    for i, canal in enumerate(ordem):
        ax.annotate(f"mediana {medianas[canal]:.0f}", (i, medianas[canal]), xytext=(0, 9),
                    textcoords="offset points", ha="center", va="bottom", fontsize=9,
                    color=brand.TEXTO_PRIMARIO,
                    bbox={"boxstyle": "round,pad=0.25", "fc": brand.SUPERFICIE, "ec": "none", "alpha": 0.9})

    ax.set_xticks(range(len(ordem)), [f"{ROTULO_CANAL.get(c, c)}\n(n={contagem[c]})" for c in ordem])
    ax.set_xlabel("")
    ax.set_ylabel("Score de mercado (Serasa)")
    ax.set_ylim(0, 1040)
    ax.set_yticks(range(0, 1001, 200))
    ax.yaxis.grid(True, color=brand.GRADE, linewidth=0.8)
    ax.set_axisbelow(True)
    sns.despine(ax=ax, left=True)
    ax.tick_params(axis="y", length=0)
    fig.suptitle("Distribuição do score de mercado por canal de aquisição", x=0.06, ha="left",
                 fontsize=13, fontweight="bold")
    ax.set_title(f"Clientes FinVarejo com score Serasa válido · referência {data_ref} · dados Serasa SIMULADOS",
                 loc="left", fontsize=9, color=brand.TEXTO_SECUNDARIO)
    fig.tight_layout()
    fig.savefig(arquivo, dpi=200)
    plt.close(fig)


def main(db_path: str, pasta_serasa: str, pasta_saida: str) -> None:
    saida = Path(pasta_saida)
    saida.mkdir(parents=True, exist_ok=True)

    serasa, qualidade = carregar_serasa(Path(pasta_serasa))
    clientes = carregar_clientes(db_path)
    base = clientes.merge(serasa, on="cpf", how="inner", validate="one_to_one")
    qualidade["clientes_base"] = len(clientes)
    qualidade["clientes_enriquecidos"] = len(base)
    qualidade["cobertura_pct"] = round(100 * len(base) / len(clientes), 1)
    qualidade["cpfs_serasa_fora_da_base"] = int((~serasa["cpf"].isin(clientes["cpf"])).sum())

    top10, excluidos = top10_upgrade(base)
    qualidade["clientes_com_score_de_dia_anterior"] = int(
        (base["data_referencia"] < base["data_referencia"].max()).sum())
    qualidade["top10_sem_filtro_excluidos_por_risco"] = excluidos
    regioes = top3_regioes_alta(base)
    grafico_score_por_canal(base, qualidade["data_referencia_mais_recente"], saida / "score_mercado_por_canal.png")

    # Saídas com dados pessoais (nome): pasta output/ é local e não versionada (LGPD)
    top10.to_csv(saida / "top10_upgrade_limite.csv", index=False, sep=";", encoding="utf-8")
    regioes.to_csv(saida / "top3_regioes_propensao_alta.csv", index=False, sep=";", encoding="utf-8")
    pd.Series(qualidade).to_csv(saida / "qualidade_serasa.csv", sep=";", header=["valor"], encoding="utf-8")

    print("== Qualidade do feed ==")
    for k, v in qualidade.items():
        print(f"  {k}: {v}")
    print("\n== Top 10 upgrade de limite ==")
    print(top10.to_string(index=False))
    print("\n== Top 3 regiões (propensão Alta) ==")
    print(regioes.to_string(index=False))


if __name__ == "__main__":
    main(*sys.argv[1:4])
