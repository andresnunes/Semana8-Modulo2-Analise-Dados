"""
"Olá, Airflow!" — o DAG mais simples possível.

Objetivo desta aula: entender os 4 conceitos básicos.

  DAG      = a receita. O desenho do fluxo: quais passos existem e em que ordem.
  Task     = um passo da receita.
  Operator = o "tipo" do passo (rodar um comando no terminal, rodar Python...).
  XCom     = o correio interno: como uma task manda um valor para a próxima.

Este DAG não tem agendamento (schedule=None): ele só roda quando VOCÊ manda.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


# O decorator @dag transforma a função abaixo na definição de um DAG.
# Metadados de configuração da DAG
@dag(
    dag_id="01_ola_airflow",            # nome que aparece na interface
    description="Primeiro contato: task de terminal, task Python e XCom",
    schedule=None,                      # None = só roda quando você clicar em "Trigger"
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"), # Permite definir o dia e hora baseado em um timezone, UTC -3 nesse caso
    catchup=False,                      # não tenta "correr atrás" de datas passadas
    tags=["aula", "01-iniciante"],      # etiquetas para filtrar na interface
)
def ola_airflow():

    # --- Task 1: um operator pronto, que roda um comando de terminal -------
    # O trecho {{ ds }} é um template do Airflow: na hora da execução ele vira
    # a data lógica da run (ex.: 2026-09-21). Existem outros: {{ ts }}, {{ dag_run }}...
    # {{}} -> template de texto baseado no jinja
    # tarefa com base na instância
    falar_oi = BashOperator(
        task_id="falar_oi",
        bash_command="echo 'Olá, Airflow! A data desta execução é {{ ds }}'",
    )

    # --- Tasks 2, 3 e 4: Python puro, usando o decorador @task -------------
    # Esse estilo se chama TaskFlow API: você escreve funções normais e o
    # Airflow cuida de transformar cada uma em uma task.
    # É a forma mais atual de definir uma Task
    @task
    def escolher_numero() -> int:
        numero = 7
        # tudo que você imprime aparece na aba "Logs" da task, na interface
        print(f"Escolhi o número {numero} e vou enviá-lo para a próxima task.")
        return numero  # o "return" vira um XCom automaticamente

    @task
    def dobrar(numero: int) -> int:
        resultado = numero * 2
        print(f"Recebi {numero} pelo XCom e dobrei: {resultado}")
        return resultado

    @task
    def anunciar(numero: int) -> None:
        print(f"Resultado final do pipeline: {numero}")
        print("Se você está lendo isso no log, o Airflow funcionou. :)")

    # --- Montando o fluxo ---------------------------------------------------
    # Chamar a função cria a task; passar o retorno para outra cria a dependência.
    numero = escolher_numero()
    dobro = dobrar(numero)

    # O operador ">>" significa "roda antes de".
    # Resultado: falar_oi -> escolher_numero -> dobrar -> anunciar
    falar_oi >> numero
    anunciar(dobro)


# Importante: não basta definir a função, é preciso CHAMAR ela para que o
# Airflow registre o DAG. Esquecer esta linha = o DAG não aparece na interface.
ola_airflow()
