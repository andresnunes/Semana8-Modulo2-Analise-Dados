# Job Glue — ETL de vendas em PySpark

[`12_vendas_glue_pyspark.ipynb`](12_vendas_glue_pyspark.ipynb) — notebook de **Glue
Interactive Session** que lê o CSV bruto do S3, limpa, deriva `receita`/`semana`, grava
Parquet particionado e registra a tabela no Glue Data Catalog.

```
s3://vendas-raw/vendas/   ──▶  PySpark (limpeza + colunas derivadas)  ──▶  s3://vendas-curated/vendas/semana=.../
                                                                            └─▶ tabela vendas_db.vendas (Athena)
```

É a mesma transformação da DAG [`11_s3_glue_athena`](../dags/11_s3_glue_athena.py), que
faz tudo em pandas dentro do worker do Airflow. A diferença: aqui o Spark distribui o
processamento, a saída sai particionada por semana e a tabela é registrada na própria
escrita (`enableUpdateCatalog`), dispensando o **Glue Crawler**.

---

## Pré-requisitos

Conta AWS real. **O LocalStack Community não emula Glue** — o mesmo limite já descrito
na DAG 11.

| O quê | Como |
|---|---|
| Buckets `vendas-raw` e `vendas-curated` | `aws s3 mb s3://vendas-raw` |
| CSV na camada raw | `aws s3 cp ../data/vendas.csv s3://vendas-raw/vendas/` |
| Banco no catálogo | `aws glue create-database --database-input Name=vendas_db` |
| Role do Glue | `AWSGlueServiceRole` + leitura/escrita nos dois buckets |

---

## Rodar

**No console** — AWS Glue Studio → *Notebooks* → *Upload notebook* → selecione o `.ipynb`
e escolha a role. Execute as células na ordem.

**Local** — Jupyter com o kernel do Glue instalado:

```bash
pip install aws-glue-sessions
jupyter lab 12_vendas_glue_pyspark.ipynb   # kernel: Glue PySpark
```

> A sessão começa a cobrar na primeira célula Python e só para no `%stop_session`
> (última célula) ou quando bate o `%idle_timeout` de 30 min. Com 2 workers `G.1X`,
> uma execução inteira custa centavos de dólar — mas **encerre a sessão**.

---

## Virar job agendado

O último bloco do notebook traz o `aws glue create-job` pronto e o
`GlueJobOperator` para disparar o job a partir da DAG 11.
