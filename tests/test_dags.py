"""Valida as DAGs das duas stacks sem precisar de Docker nem de banco.

E o que o CI roda a cada push: pega erro de sintaxe, import quebrado e as
convencoes do projeto antes do codigo chegar no Airflow.
"""

from __future__ import annotations

import pytest
from airflow.dag_processing.dagbag import DagBag

from tests.conftest import RAIZ

PASTAS = {
    "airflow-local": RAIZ / "airflow-local" / "dags",
    "airflow-aws": RAIZ / "airflow-aws" / "dags",
}

TAREFAS_ESPERADAS = {
    "05_etl_semanal": 3,
    "06_etl_robusto": 7,
    "10_s3_duckdb_local": 3,
    "11_s3_glue_athena": 5,
}


@pytest.fixture(scope="session")
def dagbags() -> dict[str, DagBag]:
    return {nome: DagBag(dag_folder=str(pasta)) for nome, pasta in PASTAS.items()}


@pytest.fixture(scope="session")
def dags(dagbags: dict[str, DagBag]) -> dict:
    return {
        dag_id: dag
        for dagbag in dagbags.values()
        for dag_id, dag in dagbag.dags.items()
    }


@pytest.mark.parametrize("stack", list(PASTAS))
def test_sem_erro_de_import(dagbags: dict[str, DagBag], stack: str) -> None:
    erros = dagbags[stack].import_errors
    assert not erros, f"{stack} tem DAG que nao carrega:\n" + "\n".join(
        f"  {arquivo}: {erro}" for arquivo, erro in erros.items()
    )


def test_todas_as_dags_esperadas_existem(dags: dict) -> None:
    assert set(dags) == set(TAREFAS_ESPERADAS)


@pytest.mark.parametrize(("dag_id", "total"), sorted(TAREFAS_ESPERADAS.items()))
def test_quantidade_de_tasks(dags: dict, dag_id: str, total: int) -> None:
    assert len(dags[dag_id].tasks) == total


def test_toda_dag_tem_tags_e_descricao(dags: dict) -> None:
    sem_metadados = [
        dag_id for dag_id, dag in dags.items() if not dag.tags or not dag.description
    ]
    assert not sem_metadados, f"DAGs sem tags ou description: {sem_metadados}"


def test_toda_task_tem_retry(dags: dict) -> None:
    sem_retry = [
        f"{dag_id}.{task.task_id}"
        for dag_id, dag in dags.items()
        for task in dag.tasks
        if task.retries < 1
    ]
    assert not sem_retry, f"tasks sem retry configurado: {sem_retry}"


def test_nenhuma_dag_faz_catchup(dags: dict) -> None:
    com_catchup = [dag_id for dag_id, dag in dags.items() if dag.catchup]
    assert not com_catchup, (
        f"DAGs com catchup ligado: {com_catchup}. "
        "Numa stack de estudo isso dispara dezenas de runs de uma vez."
    )


def test_branching_fecha_com_trigger_rule(dags: dict) -> None:
    """Sem trigger_rule permissiva, a task que junta os ramos e pulada junto."""
    consolidar = dags["06_etl_robusto"].get_task("consolidar")
    assert consolidar.trigger_rule.value == "none_failed_min_one_success"
