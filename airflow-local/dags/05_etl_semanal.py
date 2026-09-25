"""ETL semanal de vendas: le um CSV local, agrega a semana e grava o resumo."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import dag, task

CSV_ENTRADA = Path("/opt/airflow/data/vendas.csv")
DIR_SAIDA = Path("/opt/airflow/output")


def inicio_da_semana(momento) -> pd.Timestamp:
    """Segunda-feira da semana de `momento`.

    Derivar a janela da logical_date (e nao de data_interval_start) e o que faz
    `airflow dags test <dag> <data>` processar a semana daquela data: numa run
    de teste o Airflow 3 entrega data_interval_start == data_interval_end.
    """
    dia = pd.Timestamp(momento.date())
    return dia - pd.Timedelta(days=dia.weekday())


@dag(
    dag_id="05_etl_semanal",
    description="ETL semanal de vendas a partir de um CSV local",
    schedule="0 0 * * 1",
    start_date=pendulum.datetime(2026, 7, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["desafio", "etl"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def etl_semanal():
    @task
    def extrair(logical_date=None) -> list[dict]:
        df = pd.read_csv(CSV_ENTRADA, parse_dates=["data"])

        inicio = inicio_da_semana(logical_date)
        fim = inicio + pd.Timedelta(days=7)
        janela = df[(df["data"] >= inicio) & (df["data"] < fim)].copy()
        janela["data"] = janela["data"].dt.strftime("%Y-%m-%d")

        print(f"Janela {inicio.date()} ate {fim.date()}: {len(janela)} linhas")
        return janela.astype(object).where(pd.notna(janela), None).to_dict("records")

    @task
    def transformar(linhas: list[dict]) -> dict:
        df = pd.DataFrame(linhas, columns=["data", "produto", "categoria", "quantidade", "preco", "regiao"])
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")
        df["preco"] = pd.to_numeric(df["preco"], errors="coerce")

        limpo = df[df["quantidade"].notna() & (df["preco"] > 0)].copy()
        limpo["receita"] = limpo["quantidade"] * limpo["preco"]
        descartadas = len(df) - len(limpo)

        print(f"{descartadas} linha(s) descartada(s) na limpeza")
        return {
            "linhas_validas": len(limpo),
            "linhas_descartadas": descartadas,
            "receita_total": round(float(limpo["receita"].sum()), 2),
            "por_categoria": limpo.groupby("categoria")["receita"].sum().round(2).to_dict(),
            "por_regiao": limpo.groupby("regiao")["receita"].sum().round(2).to_dict(),
        }

    @task
    def carregar(agregados: dict, logical_date=None) -> str:
        DIR_SAIDA.mkdir(parents=True, exist_ok=True)
        semana = inicio_da_semana(logical_date).strftime("%Y-%m-%d")
        destino = DIR_SAIDA / f"vendas_semana_{semana}.csv"

        registros = [
            {"dimensao": dimensao, "chave": chave, "receita": receita}
            for dimensao in ("categoria", "regiao")
            for chave, receita in agregados[f"por_{dimensao}"].items()
        ]
        resumo = pd.DataFrame(registros, columns=["dimensao", "chave", "receita"])
        resumo.to_csv(destino, index=False)

        print(f"Semana de {semana}")
        print(f"  linhas validas ...... {agregados['linhas_validas']}")
        print(f"  linhas descartadas .. {agregados['linhas_descartadas']}")
        print(f"  receita total ....... R$ {agregados['receita_total']:,.2f}")
        print(f"  arquivo ............. {destino}")
        return str(destino)

    carregar(transformar(extrair()))


etl_semanal()
