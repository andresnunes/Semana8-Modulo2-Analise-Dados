# Stack local — ETL semanal, retries, branching e dynamic mapping

Airflow 3.3.2 + Postgres + LocalExecutor, rodando dois DAGs sobre um CSV de vendas.
Cobre os **desafios extras 2 e 3**.

| DAG | Desafio | O que demonstra |
|---|---|---|
| [`05_etl_semanal`](dags/05_etl_semanal.py) | 2 | ETL semanal de CSV local, testável com `dags test` |
| [`06_etl_robusto`](dags/06_etl_robusto.py) | 3 | `retries` + branching + dynamic task mapping |

> O desafio 1 (instalar e rodar o `ola_airflow`) já está feito em [`../hello-world/`](../hello-world/).
> A parte de S3/Glue/Athena é a outra stack: [`../airflow-aws/`](../airflow-aws/).

---

## Pré-requisitos

Docker Desktop instalado e **aberto**, com pelo menos 4 GB em *Settings → Resources*.

```bash
docker --version
docker compose version
```

---

## Subir

```bash
cd airflow-local

docker compose build          # 1a vez: ~3 min (baixa o Airflow + instala pandas/duckdb)
docker compose up airflow-init   # cria banco e usuario; termina com "exited with code 0"
docker compose up -d
docker compose ps             # postgres, api-server, scheduler, dag-processor
```

Interface: **<http://localhost:8080>** — usuário `airflow`, senha `airflow`.

> Se a stack [`../airflow-aws/`](../airflow-aws/) também estiver rodando, não há conflito:
> ela usa a porta 8081.

---

## Os dados

[`data/vendas.csv`](data/vendas.csv) — 856 linhas, 12 semanas (2026-07-06 a 2026-09-27):

```
data,produto,categoria,quantidade,preco,regiao
2026-07-06,Monitor,Eletronicos,12,1088.03,Sudeste
2026-07-06,HD Externo 2TB,Armazenamento,11,609.61,Sul
```

15 linhas são **sujas de propósito** (`quantidade` vazia ou `preco` igual a zero) para o ETL
ter o que limpar. Para regerar o arquivo: `python scripts/gerar_vendas.py` na raiz do repo.

Linhas por semana (o DAG 06 usa isso para escolher o ramo):

| Semana | Linhas | | Semana | Linhas |
|---|---|---|---|---|
| 2026-07-06 | 85 | | 2026-08-31 | 62 |
| 2026-07-13 | 71 | | 2026-09-07 | 67 |
| 2026-07-20 | 73 | | 2026-09-14 | 72 |
| 2026-07-27 | 85 | | 2026-09-21 | 76 |
| 2026-08-03 | 76 | | 2026-08-17 | 68 |
| 2026-08-10 | 57 | | 2026-08-24 | 64 |

---

## Testando com `dags test`

`airflow dags test` roda o DAG inteiro na hora, com o log direto no terminal e **sem precisar
ligar o schedule na interface**. É a forma mais rápida de validar.

### DAG 05 — ETL semanal

```bash
docker compose exec airflow-scheduler airflow dags test 05_etl_semanal 2026-08-03
```

Saída esperada:

```
Janela 2026-08-03 ate 2026-08-10: 76 linhas
0 linha(s) descartada(s) na limpeza
Semana de 2026-08-03
  linhas validas ...... 76
  linhas descartadas .. 0
  receita total ....... R$ 553,079.45
  arquivo ............. /opt/airflow/output/vendas_semana_2026-08-03.csv
```

O resultado aparece em [`output/`](output/):

```bash
cat output/vendas_semana_2026-08-03.csv
```

```csv
dimensao,chave,receita
categoria,Mobiliario,188403.28
categoria,Eletronicos,249998.26
...
regiao,Sul,202875.5
```

Troque a data para processar outra semana — qualquer dia da semana serve, o DAG
puxa para a segunda-feira correspondente:

```bash
docker compose exec airflow-scheduler airflow dags test 05_etl_semanal 2026-08-10
# 57 linhas, 1 descartada (linha suja), 56 validas
```

### DAG 06 — retries, branching e mapping

O ramo depende do volume da semana: **≥ 70 linhas** processa em lotes, **< 70** processa
de uma vez. Por isso dá para exercitar os dois:

```bash
# ramo "lotes" — 76 linhas
docker compose exec airflow-scheduler airflow dags test 06_etl_robusto 2026-08-03
```

```
fonte validada: 856 linhas
Janela 2026-08-03 ate 2026-08-10: 76 linhas
76 linhas (>= 70): processando em lotes
4 categorias -> 4 lote(s)
lote ['Armazenamento']: 27 linhas | R$ 79,076.04
lote ['Eletronicos']: 16 linhas | R$ 249,998.26
lote ['Mobiliario']: 14 linhas | R$ 188,403.28
lote ['Perifericos']: 19 linhas | R$ 35,601.87
ramo executado ... lotes (4 resultado(s))
receita total .... R$ 553,079.45
```

```bash
# ramo "simples" — 57 linhas
docker compose exec airflow-scheduler airflow dags test 06_etl_robusto 2026-08-10
```

```
57 linhas (< 70): processando de uma vez
semana inteira: 56 linhas | R$ 453,273.75
ramo executado ... simples (1 resultado(s))
```

> A receita total do DAG 06 no ramo de lotes bate exatamente com a do DAG 05 na mesma
> semana (R$ 553.079,45). É a prova de que dividir e reconsolidar não perdeu nada.

---

## Como os três recursos do desafio 3 aparecem no DAG 06

### `retries`

Nos `default_args`, valendo para todas as tasks:

```python
default_args={"retries": 2, "retry_delay": timedelta(seconds=30)}
```

E sobrescrito na task que lê a fonte, que é a mais sujeita a falha transitória:

```python
@task(retries=3, retry_delay=timedelta(seconds=10))
def validar_fonte() -> int:
```

Para ver um retry acontecendo de verdade, renomeie o CSV e rode o DAG: `validar_fonte`
falha, espera 10s e tenta de novo, 3 vezes.

### Branching

```python
@task.branch
def decidir_volume(linhas: list[dict]) -> str:
    if len(linhas) >= LIMITE_PARA_LOTES:
        return "dividir_em_lotes"
    return "processar_simples"
```

`@task.branch` devolve o **task_id** do caminho escolhido; o outro ramo fica `skipped`.
Os descendentes do ramo escolhido rodam normalmente.

A pegadinha clássica está no fechamento dos ramos. A task final precisa de
`trigger_rule`, senão ela também é pulada — a regra padrão (`all_success`) considera
o ramo pulado como "não teve sucesso":

```python
@task(trigger_rule="none_failed_min_one_success")
def consolidar() -> dict:
```

### Dynamic task mapping

```python
mapeadas = processar_lote.expand(lote=lotes)
```

`dividir_em_lotes` devolve uma lista, e o Airflow cria **uma instância de `processar_lote`
por item** em tempo de execução. Com `CATEGORIAS_POR_LOTE = 1` e 4 categorias no CSV, saem
4 tasks mapeadas. Na interface elas aparecem agrupadas, com um seletor de índice.

---

## Estrutura

```
airflow-local/
├── docker-compose.yaml   # Airflow 3 + Postgres, LocalExecutor
├── Dockerfile            # imagem oficial + pandas, pyarrow, duckdb
├── requirements.txt
├── dags/
│   ├── 05_etl_semanal.py
│   └── 06_etl_robusto.py
├── data/vendas.csv       # entrada (montado read-only no container)
└── output/               # saida do ETL
```

Por que `Dockerfile` em vez de `_PIP_ADDITIONAL_REQUIREMENTS`: a variável de ambiente
reinstala os pacotes a cada start de **cada** container, o que atrasa o boot e estoura
os healthchecks. A imagem resolve isso uma vez só.

Só o serviço `airflow-init` tem `build:`; os outros reusam a tag `airflow-local:3.3.2`.
Buildar nos quatro em paralelo faz o Docker brigar pela mesma tag e falhar com
`image ... already exists`.

---

## Parar

```bash
docker compose down      # desliga, mantem o historico
docker compose down -v   # apaga tambem o banco de metadados
```

---

## Se der problema

| Sintoma | O que fazer |
|---|---|
| `port is already allocated` | troque `"8080:8080"` por outra porta no `docker-compose.yaml` |
| `image ... already exists` no build | só o `airflow-init` deve ter `build:` — confira o compose |
| A interface não abre | espere ~30s; depois `docker compose logs airflow-api-server \| tail -30` |
| O DAG não aparece | espere ~30s (o dag-processor relê a pasta) e atualize a página |
| `ModuleNotFoundError: pandas` | a imagem não foi buildada: `docker compose build` |
| DAG roda mas processa 0 linhas | a data pedida está fora de 2026-07-06 → 2026-09-27 |
| `output/` vazio | o `dags test` grava dentro do container; confira o bind mount `./output` |
| Login recusado | rode `docker compose up airflow-init` de novo |
| Containers reiniciando | falta memória: Docker Desktop → *Settings → Resources* → 4 GB |

---

## Nota sobre segredos

Senha do Postgres, login do Airflow e `JWT_SECRET` estão em texto puro no
`docker-compose.yaml`. É aceitável **porque a stack é local e descartável**. Em qualquer
ambiente compartilhado isso vira variável de ambiente ou Secrets Manager — é exatamente o
que a stack [`../airflow-aws/`](../airflow-aws/) faz com as credenciais AWS.
