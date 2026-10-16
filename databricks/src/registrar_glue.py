# Databricks notebook source
# Registra a Gold no AWS Glue Data Catalog para consumidores AWS nativos
# (Athena, SageMaker, QuickSight). O Unity Catalog continua a fonte de verdade de
# governança dentro do Databricks; o Glue aponta para o MESMO caminho Delta em S3
# (sem cópia). Permissões no Glue via Lake Formation (somente leitura, por grupo).
#
# A DDL via Athena cria a tabela no Glue com table_type=DELTA: o schema é lido do
# _delta_log, então mudanças compatíveis de schema não exigem novo registro.
# Credenciais: instance profile / UC service credential com athena:StartQueryExecution,
# glue:CreateTable/GetTable e acesso ao bucket de resultados do Athena.

import time

import boto3

dbutils.widgets.text("gold_path", "s3://finvarejo-lakehouse-dev/gold/")
dbutils.widgets.text("glue_database", "finvarejo_gold_dev")
GOLD_PATH = dbutils.widgets.get("gold_path").rstrip("/")
GLUE_DB = dbutils.widgets.get("glue_database")
ATHENA_OUTPUT = "s3://finvarejo-athena-results/registro-glue/"

TABELAS = {"features_cliente_serasa": f"{GOLD_PATH}/features_cliente_serasa"}

athena = boto3.client("athena", region_name="sa-east-1")


def executar(sql: str) -> None:
    qid = athena.start_query_execution(
        QueryString=sql, ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )["QueryExecutionId"]
    while True:
        estado = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if estado["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {estado['State']}: {estado.get('StateChangeReason')}\n{sql}")


# COMMAND ----------

executar(f"CREATE DATABASE IF NOT EXISTS {GLUE_DB} COMMENT 'Gold FinVarejo (Delta, fonte: Databricks)'")
for tabela, caminho in TABELAS.items():
    executar(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {GLUE_DB}.{tabela}
        LOCATION '{caminho}'
        TBLPROPERTIES ('table_type' = 'DELTA')
    """)
    print(f"Registrada no Glue: {GLUE_DB}.{tabela} -> {caminho}")
