"""ETL semanal com retries, branching e dynamic task mapping."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import dag, get_current_context, task

CSV_ENTRADA = Path("/opt/airflow/data/vendas.csv")
LIMITE_PARA_LOTES = 70
CATEGORIAS_POR_LOTE = 1

COLUNAS = ["data", "produto", "categoria", "quantidade", "preco", "regiao"]


def inicio_da_semana(momento) -> pd.Timestamp:
    """Segunda-feira da semana de `momento`.

    Derivar a janela da logical_date (e nao de data_interval_start) e o que faz
    `airflow dags test <dag> <data>` processar a semana daquela data: numa run
    de teste o Airflow 3 entrega data_interval_start == data_interval_end.
    """
    dia = pd.Timestamp(momento.date())
    return dia - pd.Timedelta(days=dia.weekday())

# essa função é externa a DAG
def preparar(linhas: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(linhas, columns=COLUNAS)
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")
    df["preco"] = pd.to_numeric(df["preco"], errors="coerce")

    limpo = df[df["quantidade"].notna() & (df["preco"] > 0)].copy()
    limpo["receita"] = limpo["quantidade"] * limpo["preco"]
    return limpo


@dag(
    dag_id="06_etl_robusto",
    description="ETL semanal com retries, branching e dynamic task mapping",
    schedule="0 0 * * 1",
    start_date=pendulum.datetime(2026, 7, 6, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["desafio", "etl", "avancado"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
)
def etl_robusto():
    # Ela sobreescreve o padrão de retries e retry_delay
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def validar_fonte() -> int:
        if not CSV_ENTRADA.exists():
            raise FileNotFoundError(f"CSV nao encontrado em {CSV_ENTRADA}")

        with CSV_ENTRADA.open(encoding="utf-8") as arquivo:
            total = sum(1 for _ in arquivo) - 1
        if total <= 0:
            raise ValueError("CSV sem linhas de dados")

        print(f"fonte validada: {total} linhas")
        return total

    @task
    def extrair(logical_date=None) -> list[dict]:
        df = pd.read_csv(CSV_ENTRADA, parse_dates=["data"])

        inicio = inicio_da_semana(logical_date)
        fim = inicio + pd.Timedelta(days=7)
        janela = df[(df["data"] >= inicio) & (df["data"] < fim)].copy()
        janela["data"] = janela["data"].dt.strftime("%Y-%m-%d")

        print(f"Janela {inicio.date()} ate {fim.date()}: {len(janela)} linhas")
        return janela.astype(object).where(pd.notna(janela), None).to_dict("records")

    @task.branch # Cria um paralelismos de tarefa
    def decidir_volume(linhas: list[dict]) -> str:
        if len(linhas) >= LIMITE_PARA_LOTES:
            print(f"{len(linhas)} linhas (>= {LIMITE_PARA_LOTES}): processando em lotes")
            return "dividir_em_lotes"

        print(f"{len(linhas)} linhas (< {LIMITE_PARA_LOTES}): processando de uma vez")
        return "processar_simples"

# Duas tarefas que podem ou não acontecer
# será uma ou a outra
# As 3 tarefas a seguir tem caminhos diferentes para executar
    @task
    def dividir_em_lotes(linhas: list[dict]) -> list[list[dict]]:
        categorias = sorted({linha["categoria"] for linha in linhas})
        lotes = [
            [linha for linha in linhas if linha["categoria"] in grupo]
            for grupo in (
                set(categorias[i : i + CATEGORIAS_POR_LOTE])
                for i in range(0, len(categorias), CATEGORIAS_POR_LOTE)
            )
        ]
        print(f"{len(categorias)} categorias -> {len(lotes)} lote(s)")
        return lotes

    @task
    def processar_lote(lote: list[dict]) -> dict:
        limpo = preparar(lote)
        categorias = sorted(limpo["categoria"].unique().tolist())
        receita = round(float(limpo["receita"].sum()), 2)

        print(f"lote {categorias}: {len(limpo)} linhas | R$ {receita:,.2f}")
        return {"linhas": len(limpo), "receita": receita, "categorias": categorias}

    @task
    def processar_simples(linhas: list[dict]) -> dict:
        limpo = preparar(linhas)
        categorias = sorted(limpo["categoria"].unique().tolist())
        receita = round(float(limpo["receita"].sum()), 2)

        print(f"semana inteira: {len(limpo)} linhas | R$ {receita:,.2f}")
        return {"linhas": len(limpo), "receita": receita, "categorias": categorias}
    
# espera um resultado das tarefas anteriores
#   nenhuma tarefa falhou e temos pelo menos uma tarefa com sucesso
    @task(trigger_rule="none_failed_min_one_success")
    def consolidar() -> dict:
        ti = get_current_context()["ti"]

        parciais = [p for p in (ti.xcom_pull(task_ids="processar_lote") or []) if p]
        ramo = "lotes"
        if not parciais:
            simples = ti.xcom_pull(task_ids="processar_simples")
            parciais = [simples] if simples else []
            ramo = "simples"

        linhas = sum(p["linhas"] for p in parciais)
        receita = round(sum(p["receita"] for p in parciais), 2)
        categorias = sorted({c for p in parciais for c in p["categorias"]})

        print(f"ramo executado ... {ramo} ({len(parciais)} resultado(s))")
        print(f"linhas validas ... {linhas}")
        print(f"categorias ....... {', '.join(categorias)}")
        print(f"receita total .... R$ {receita:,.2f}")
        return {"ramo": ramo, "linhas": linhas, "receita": receita}

    dados = extrair()
    validar_fonte() >> dados

    escolha = decidir_volume(dados) # define a tarefa a ser executada
    lotes = dividir_em_lotes(dados)
    simples = processar_simples(dados)
    escolha >> [lotes, simples]

    mapeadas = processar_lote.expand(lote=lotes)
    [mapeadas, simples] >> consolidar() # O resultado é que ou mapeadas, ou simples deu sucesso


etl_robusto()
