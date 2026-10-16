# Databricks notebook source
# Exemplo para Data Science: montar o dataset de treino SEM data leakage.
#
# Cada rótulo tem uma data de observação (quando a decisão de crédito foi tomada).
# O FeatureLookup com timestamp_lookup_key faz um as-of join: para cada
# (id_cliente, data_observacao) pega a última feature com ts_feature <= data_observacao.
# Nada publicado depois da decisão entra no treino. lookback_window descarta
# features velhas demais (cliente sem Serasa recente vira nulo, não um valor antigo).

from datetime import timedelta

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
CATALOG = "finvarejo_prod"

# Rótulos: 1 linha por decisão (id_cliente, data_observacao, inadimpliu_90d)
rotulos = spark.table(f"{CATALOG}.risco.rotulos_inadimplencia_90d")

training_set = fe.create_training_set(
    df=rotulos,
    feature_lookups=[
        FeatureLookup(
            table_name=f"{CATALOG}.gold.features_cliente_serasa",
            lookup_key="id_cliente",
            timestamp_lookup_key="data_observacao",
            lookback_window=timedelta(days=7),
            feature_names=[
                "score_mercado",
                "score_interno",
                "dif_score_mercado_interno",
                "faixa_renda_ordinal",
                "propensao_credito_ordinal",
                "var_score_mercado_ult_obs",
            ],
        )
    ],
    label="inadimpliu_90d",
)
df_treino = training_set.load_df()

# Ao logar o modelo com fe.log_model(..., training_set=training_set), o modelo guarda a
# linhagem das features e o score_batch busca as features automaticamente na inferência,
# com a mesma lógica do treino (sem divergência treino/produção).
