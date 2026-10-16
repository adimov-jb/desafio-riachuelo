"""Glue 5.0 (Spark): copia o Export Raw User Data do CRM para o bucket raw da FinVarejo.

Objetivo: tirar até 2 TB do link temporário (expira em 24h) o mais rápido possível.
Depois disso o link não importa mais: todo reprocesso parte do nosso S3.

- Paraleliza por arquivo entre os executores (o export vem em N arquivos).
- Link s3:// (cross-account): cópia server-side (UploadPartCopy) -> nenhum byte passa pelo job.
- Link https (pré-assinado): download em streaming + multipart upload de 128 MB, sem disco.
- Retomável e idempotente: chave de destino determinística; arquivo já presente com o
  mesmo tamanho (e sha256, quando o manifesto traz) é pulado. Uma retentativa do MWAA
  não recopia o que já chegou.
- Arquivos chegam como vieram (ex.: .json.gz) na zona raw imutável (versionamento +
  Object Lock em modo governance por 30 dias): é a evidência do que o CRM entregou.
"""

import hashlib
import sys
from urllib.parse import urlparse

import boto3
import requests
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from boto3.s3.transfer import TransferConfig
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["export_date", "tabela_controle", "bucket_raw"])
REGIAO = "sa-east-1"
CONFIG_TRANSFER = TransferConfig(multipart_chunksize=128 * 1024 * 1024, max_concurrency=8)

sc = SparkContext()
glue = GlueContext(sc)

item = boto3.resource("dynamodb", region_name=REGIAO).Table(args["tabela_controle"]).get_item(
    Key={"export_date": args["export_date"]}, ConsistentRead=True
)["Item"]
arquivos = [dict(f) for f in item["links"]]  # [{nome, url, tamanho_bytes, sha256?}]
prefixo = f"export_raw/export_date={args['export_date']}/"
bucket = args["bucket_raw"]


class _LeitorComHash:
    """Envolve o stream HTTP calculando sha256 enquanto o boto3 faz o multipart."""

    def __init__(self, raw):
        self.raw, self.sha = raw, hashlib.sha256()

    def read(self, n=-1):
        bloco = self.raw.read(n)
        self.sha.update(bloco)
        return bloco


def copiar(arquivo: dict) -> tuple[str, int, str]:
    s3 = boto3.client("s3", region_name=REGIAO)
    destino = prefixo + arquivo["nome"]
    tamanho = int(arquivo["tamanho_bytes"])
    try:
        cab = s3.head_object(Bucket=bucket, Key=destino)
        if cab["ContentLength"] == tamanho and cab["Metadata"].get("sha256") in (None, arquivo.get("sha256")):
            return destino, tamanho, "ja_copiado"
    except s3.exceptions.ClientError:
        pass

    url = arquivo["url"]
    meta = {"Metadata": {"origem": "crm_export_raw", "export_date": args["export_date"]},
            "ServerSideEncryption": "aws:kms"}
    if url.startswith("s3://"):
        origem = urlparse(url)
        s3.copy({"Bucket": origem.netloc, "Key": origem.path.lstrip("/")}, bucket, destino,
                ExtraArgs=meta, Config=CONFIG_TRANSFER)
    else:
        with requests.get(url, stream=True, timeout=(10, 300)) as resp:
            resp.raise_for_status()
            leitor = _LeitorComHash(resp.raw)
            s3.upload_fileobj(leitor, bucket, destino, ExtraArgs=meta, Config=CONFIG_TRANSFER)
        if arquivo.get("sha256") and leitor.sha.hexdigest() != arquivo["sha256"]:
            s3.delete_object(Bucket=bucket, Key=destino)
            raise ValueError(f"sha256 divergente em {arquivo['nome']}")
    return destino, tamanho, "copiado"


# Um arquivo por task; os maiores primeiro para não sobrar um arquivo gigante no fim
arquivos.sort(key=lambda f: -int(f["tamanho_bytes"]))
resultado = sc.parallelize(arquivos, numSlices=max(1, len(arquivos))).map(copiar).collect()

copiados = sum(1 for _, _, s in resultado if s == "copiado")
print(f"{len(resultado)} arquivos ({sum(t for _, t, _ in resultado) / 1e9:.1f} GB); {copiados} copiados agora")
