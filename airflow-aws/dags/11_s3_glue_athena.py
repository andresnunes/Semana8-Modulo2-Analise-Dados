"""Pipeline S3 -> Glue Crawler -> Athena, contra a AWS de verdade.

Nao roda no LocalStack Community: Glue e Athena sao do plano Ultimate.
O passo a passo para provisionar a conta esta no README, Parte B.
"""

from __future__ import annotations

import os
import tempfile
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sdk import dag, task

CSV_ENTRADA = Path("/opt/airflow/data/vendas.csv")

BUCKET_RAW = os.environ.get("BUCKET_RAW", "vendas-raw")
BUCKET_CURATED = os.environ.get("BUCKET_CURATED", "vendas-curated")
BUCKET_ATHENA = os.environ.get("BUCKET_ATHENA", "athena-results")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "vendas_db")
GLUE_CRAWLER = os.environ.get("GLUE_CRAWLER", "vendas-crawler")
GLUE_ROLE = os.environ.get("GLUE_ROLE", "")

PREFIXO_CURATED = "vendas"
CHAVE_RAW = "vendas/vendas.csv"
CHAVE_CURATED = f"{PREFIXO_CURATED}/vendas.parquet"

CONSULTA = f"""
SELECT categoria,
       count(*) AS pedidos,
       round(sum(receita), 2) AS receita
FROM {PREFIXO_CURATED}
GROUP BY categoria
ORDER BY receita DESC
"""


@dag(
    dag_id="11_s3_glue_athena",
    description="S3 + Glue Crawler + Athena na AWS real",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["desafio", "aws"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def s3_glue_athena():
    @task
    def upload_raw() -> str:
        hook = S3Hook(aws_conn_id="aws_default")
        hook.load_file(
            filename=str(CSV_ENTRADA),
            key=CHAVE_RAW,
            bucket_name=BUCKET_RAW,
            replace=True,
        )
        print(f"enviado: s3://{BUCKET_RAW}/{CHAVE_RAW}")
        return CHAVE_RAW

    @task
    def transformar_para_parquet(chave: str) -> str:
        hook = S3Hook(aws_conn_id="aws_default")

        with tempfile.TemporaryDirectory() as tmp:
            baixado = hook.download_file(key=chave, bucket_name=BUCKET_RAW, local_path=tmp)
            df = pd.read_csv(baixado, parse_dates=["data"])

            df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")
            df["preco"] = pd.to_numeric(df["preco"], errors="coerce")
            limpo = df[df["quantidade"].notna() & (df["preco"] > 0)].copy()
            limpo["receita"] = (limpo["quantidade"] * limpo["preco"]).round(2)
            limpo["semana"] = (
                limpo["data"] - pd.to_timedelta(limpo["data"].dt.weekday, unit="D")
            ).dt.strftime("%Y-%m-%d")
            limpo["data"] = limpo["data"].dt.strftime("%Y-%m-%d")

            destino = Path(tmp) / "vendas.parquet"
            limpo.to_parquet(destino, index=False)
            hook.load_file(
                filename=str(destino),
                key=CHAVE_CURATED,
                bucket_name=BUCKET_CURATED,
                replace=True,
            )

        print(f"{len(df) - len(limpo)} linha(s) descartada(s) na limpeza")
        print(f"gravado: s3://{BUCKET_CURATED}/{CHAVE_CURATED} ({len(limpo)} linhas)")
        return CHAVE_CURATED

    # Task
    rodar_crawler = GlueCrawlerOperator(
        task_id="rodar_crawler",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        config={
            "Name": GLUE_CRAWLER,
            "Role": GLUE_ROLE,
            "DatabaseName": GLUE_DATABASE,
            "Targets": {"S3Targets": [{"Path": f"s3://{BUCKET_CURATED}/{PREFIXO_CURATED}/"}]},
        },
    )

    # Task
    consultar_athena = AthenaOperator(
        task_id="consultar_athena",
        aws_conn_id="aws_default",
        database=GLUE_DATABASE,
        query=CONSULTA,
        output_location=f"s3://{BUCKET_ATHENA}/resultados/",
    )

    @task
    def mostrar_resultado(query_execution_id: str) -> list[dict]:
        hook = S3Hook(aws_conn_id="aws_default")
        chave = f"resultados/{query_execution_id}.csv"

        with tempfile.TemporaryDirectory() as tmp:
            baixado = hook.download_file(key=chave, bucket_name=BUCKET_ATHENA, local_path=tmp)
            resultado = pd.read_csv(baixado)

        print(resultado.to_string(index=False))
        return resultado.to_dict("records")

    transformar_para_parquet(upload_raw()) >> rodar_crawler >> consultar_athena
    mostrar_resultado(consultar_athena.output)


s3_glue_athena()
