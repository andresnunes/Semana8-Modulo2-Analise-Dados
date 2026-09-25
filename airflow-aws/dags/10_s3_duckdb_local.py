"""Pipeline S3 -> Parquet -> consulta analitica, rodando inteiro no LocalStack.

O DuckDB faz aqui o papel do Athena: o LocalStack Community emula S3, mas Glue e
Athena so existem no plano Ultimate. A versao com Glue/Athena de verdade esta em
11_s3_glue_athena.py.
"""

from __future__ import annotations

import os
import tempfile
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

CSV_ENTRADA = Path("/opt/airflow/data/vendas.csv")
BUCKET_RAW = os.environ.get("BUCKET_RAW", "vendas-raw")
BUCKET_CURATED = os.environ.get("BUCKET_CURATED", "vendas-curated")
CHAVE_RAW = "vendas/vendas.csv"
CHAVE_CURATED = "vendas/vendas.parquet"


@dag(
    dag_id="10_s3_duckdb_local",
    description="S3 no LocalStack + DuckDB no papel do Athena",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["desafio", "aws", "localstack"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
)
def s3_duckdb_local():
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

    @task
    def consultar_duckdb(chave: str) -> list[dict]:
        hook = S3Hook(aws_conn_id="aws_default")

        with tempfile.TemporaryDirectory() as tmp:
            baixado = hook.download_file(key=chave, bucket_name=BUCKET_CURATED, local_path=tmp)
            df = pd.read_parquet(baixado)  # noqa: F841 - lido pelo DuckDB por nome

            resultado = duckdb.sql(
                """
                SELECT categoria,
                       count(*) AS pedidos,
                       round(sum(receita), 2) AS receita
                FROM df
                GROUP BY categoria
                ORDER BY receita DESC
                """
            ).df()

        print(resultado.to_string(index=False))
        return resultado.to_dict("records")

    consultar_duckdb(transformar_para_parquet(upload_raw()))


s3_duckdb_local()
