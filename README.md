# Semana 8 — Módulo 2 — Análise de Dados

Airflow 3 aplicado a ETL, orquestração e AWS. Três stacks Docker independentes, todas locais.


## Ordem de estudos:

1. Slides
2. hello-world desse repo
3. airflow-local - complementa o hello e exemplifica os slides
4. Docs do MWAA da AWS - https://docs.aws.amazon.com/pt_br/mwaa/latest/userguide/get-started.html
5. airflow-aws - exemplifica uma execução da AWS
6. terraform - pode ser executado, com cautela para não gerar gastos
7. tests - pesquisa sobre pytest (OPCIONAL)

## Listagem de pastas

| Pasta | Porta | O que é |
|---|---|---|
| [`hello-world/`](hello-world/) | 8081 | Material das aulas 1 e 2 |
| [`airflow-local/`](airflow-local/) | 8080 | **Desafios 2 e 3** — ETL semanal, retries, branching, dynamic mapping |
| [`airflow-aws/`](airflow-aws/) | 8082 | **Desafio 4** — S3, Glue e Athena |

As três podem rodar ao mesmo tempo: cada uma tem sua própria rede, seu Postgres e sua porta.

Fora delas, [`terraform/`](terraform/) cria na AWS a infraestrutura que o `airflow-aws`
consome, e [`tests/`](tests/) + [`.github/`](.github/workflows/airflow-ci.yml) cobrem o CI.

---

## Desafios extras

| # | Desafio | Onde está | Status |
|---|---|---|---|
| 1 | Instalar localmente e rodar o `ola_airflow` | [`hello-world/`](hello-world/) | feito nas aulas |
| 2 | ETL semanal com CSV próprio, testado com `dags test` | [`05_etl_semanal.py`](airflow-local/dags/05_etl_semanal.py) | ✅ |
| 3 | Retries, branching e dynamic mapping | [`06_etl_robusto.py`](airflow-local/dags/06_etl_robusto.py) | ✅ |
| 4 | S3 / Glue / Athena com MWAA ou LocalStack | [`airflow-aws/`](airflow-aws/) | ✅ local + roteiro AWS |
| 5 | Automatizar testes e deploy com CI/CD | [`.github/workflows/`](.github/workflows/airflow-ci.yml) | ✅ CI ativo, deploy documentado |
| + | Infra como código (extra) | [`terraform/`](terraform/) | ✅ |

---

## Começando

```bash
# Desafios 2 e 3
cd airflow-local
docker compose build && docker compose up airflow-init && docker compose up -d
docker compose exec airflow-scheduler airflow dags test 05_etl_semanal 2026-08-03
```

```bash
# Desafio 4
cd airflow-aws
docker compose build && docker compose up airflow-init
docker compose --profile local up -d
docker compose exec airflow-scheduler airflow dags test 10_s3_duckdb_local
```

Login das três interfaces: `airflow` / `airflow`.

Instruções completas: [README do airflow-local](airflow-local/README.md) ·
[README do airflow-aws](airflow-aws/README.md).

---

## Duas decisões que valem explicar

**Por que o desafio 4 tem dois DAGs.** O LocalStack Community emula S3, mas **Glue e Athena
são do plano Ultimate**, pago. Então há um DAG que roda de graça na sua máquina (S3 real do
LocalStack + DuckDB no papel do Athena) e outro com Glue/Athena de verdade, mais o passo a
passo para provisionar a conta AWS. Custo do exercício na AWS: **menos de US$ 0,05**.

**Por que não MWAA.** O MWAA roda a mesma DAG sem alteração (já suporta Airflow 3.3.1), mas
**não tem free tier**: ~US$ 0,49/hora, ~US$ 430/mês se ficar ligado. A trilha principal é
Airflow local apontando para os serviços AWS reais, que cumpre o mesmo objetivo por centavos.
Detalhes em [airflow-aws/README.md](airflow-aws/README.md#e-o-mwaa).

---

## Desafio 5 — CI/CD

[`.github/workflows/airflow-ci.yml`](.github/workflows/airflow-ci.yml) roda a cada push e PR:

| Job | O que faz |
|---|---|
| `lint` | `ruff check` + valida os dois `docker-compose.yaml` |
| `terraform` | `terraform fmt -check` + `validate`, sem tocar em nenhuma conta AWS |
| `test` | `pytest tests/` — carrega todas as DAGs e checa as convenções |
| `deploy` | `aws s3 sync` das DAGs para o bucket do MWAA via OIDC — **desativado** |

Os testes em [`tests/test_dags.py`](tests/test_dags.py) pegam, sem Docker e sem banco:

- DAG que não importa (erro de sintaxe, dependência faltando, import de Airflow 2);
- DAG sem `tags` ou `description`;
- task sem `retries`;
- DAG com `catchup` ligado, que dispararia dezenas de runs de uma vez;
- a `trigger_rule` do branching, que se esquecida faz a task final ser pulada em silêncio.

Rodando na mão:

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
ruff check .
```

O job de `deploy` está com `if: false` porque depende de recursos que precisam existir na
sua conta AWS (OIDC provider, IAM role, bucket). O passo a passo para ativá-lo está
comentado no próprio arquivo do workflow. Ele usa **OIDC em vez de access key**: o GitHub
troca um token de curta duração por credenciais temporárias, então não existe secret
permanente no repositório para vazar.

---

## Infra como código

[`terraform/`](terraform/) cria na AWS os buckets, o Glue Database, o crawler, a role do
Glue e o usuário IAM que o `airflow-aws` consome — e gera o `.env.aws` já preenchido:

```bash
cd terraform
cp exemplo.tfvars meu.tfvars
terraform init
terraform apply -var-file=meu.tfvars
terraform output -raw env_aws > ../airflow-aws/.env.aws
```

Para desmontar: `terraform destroy -var-file=meu.tfvars`.

É o mesmo resultado dos passos manuais da Parte B do
[README do airflow-aws](airflow-aws/README.md) — vale fazer na mão primeiro e depois
comparar com o código. Detalhes em [terraform/README.md](terraform/README.md).

---

## Os dados

[`scripts/gerar_vendas.py`](scripts/gerar_vendas.py) gera o `vendas.csv` usado pelas duas
stacks novas — 856 linhas, 12 semanas (2026-07-06 a 2026-09-27), determinístico. Inclui
15 linhas sujas de propósito (`quantidade` vazia ou `preco` zerado) para o ETL ter o que
limpar e para o branching ter o que decidir.

```bash
python scripts/gerar_vendas.py    # regera nas duas pastas data/
```
