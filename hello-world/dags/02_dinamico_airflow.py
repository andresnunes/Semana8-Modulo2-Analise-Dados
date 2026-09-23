from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator # Executa código no terminal

@dag(
        schedule="@daily",
        start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo")
) 
def pipeline_dinamico(): 
    @task 
    def listar_arquivos() -> list[str]: return ["a.csv", "b.csv", "c.csv"] 

    @task 
    def processar(arquivo: str): 
        print(f"Processando {arquivo}") 

    # um lambda pode ser usado como parametro de outra função
    # A lista resultante da  vira 3 tasks
    #   para cada item da lista nos executamos um processar
    processar.expand(arquivo=listar_arquivos()) 

    # arquivo=listar_arquivos() -> Esse processo é construido pelo XCOM - Cross Comunication, Comunicação cruzada

pipeline_dinamico()
