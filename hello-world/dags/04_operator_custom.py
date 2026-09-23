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
from airflow.sdk import BaseOperator

# Operator customizado
# Ele é a base das tarefas do Airflow
# Todos os métodos de BaseOperator e todos os atributos são herdados pelo MeuOperador, incluindo o consrutor
class MeuOperator(BaseOperator):
    template_fields = ("caminho",)

    def __init__(self, caminho, **kwargs):
        # O super term todos os itens que um Operador padrão tem
        # Nesse caso isso inclui o task_id
        # por consequencia o MeuOperador tem o atributo task_id
        super().__init__(**kwargs)
        self.caminho = caminho

    def execute(self, context):
        print(self.caminho)  # já vem renderizado aqui

# O decorator @dag transforma a função abaixo na definição de um DAG.
# Metadados de configuração da DAG
@dag(
    dag_id="04_operator",            # nome que aparece na interface
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


    operador_custom = MeuOperator(
        task_id="custom", 
        caminho="/dados/{{ ds }}/arquivo.csv" # usa o template do Jinja
    )

    falar_oi >> operador_custom

ola_airflow()
