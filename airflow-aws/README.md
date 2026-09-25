# Stack AWS — S3, Glue e Athena

Airflow 3.3.2 + Postgres + LocalExecutor + LocalStack. Cobre o **desafio extra 4**.

| DAG | Onde roda | Custo |
|---|---|---|
| [`10_s3_duckdb_local`](dags/10_s3_duckdb_local.py) | LocalStack, na sua máquina | zero |
| [`11_s3_glue_athena`](dags/11_s3_glue_athena.py) | AWS de verdade | < US$ 0,05 |

---

## Por que dois DAGs

O LocalStack **Community (grátis) emula S3, mas não emula Glue nem Athena** — os dois são
do plano *Ultimate*, pago. ([docs](https://docs.localstack.cloud/aws/services/athena/))

Em vez de fingir que o desafio roda de graça, a stack tem dois caminhos:

- **DAG 10** roda inteiro na sua máquina. O S3 é real (boto3 de verdade, só apontado para o
  LocalStack); no lugar do Glue Catalog + Athena entra o **DuckDB**, que faz a mesma consulta
  analítica sobre o Parquet.
- **DAG 11** é o pipeline de verdade: `S3 → Glue Crawler → Athena`. A **Parte B** deste README
  é o passo a passo para provisionar a conta e rodá-lo.

Os dois fazem a mesma coisa — sobem o CSV, limpam, gravam Parquet, consultam. O que muda é
quem responde a consulta.

```
DAG 10   upload_raw ──▶ transformar_para_parquet ──▶ consultar_duckdb
DAG 11   upload_raw ──▶ transformar_para_parquet ──▶ rodar_crawler ──▶ consultar_athena ──▶ mostrar_resultado
```

---

# Parte A — Rodando local (LocalStack, grátis)

## Subir

```bash
cd airflow-aws

docker compose build
docker compose up airflow-init
docker compose --profile local up -d
docker compose ps
```

Interface: **<http://localhost:8082>** — usuário `airflow`, senha `airflow`.

> `--profile local` é o que sobe o LocalStack. Sem ele o LocalStack não sobe — é assim
> que a Parte B aponta para a AWS de verdade sem precisar editar o compose.
>
> Se a porta 8082 estiver ocupada, troque `AIRFLOW_PORT` no [`.env`](.env). As stacks
> [`../hello-world/`](../hello-world/) e [`../airflow-local/`](../airflow-local/) usam
> 8081 e 8080.

O LocalStack cria os três buckets sozinho no boot, via
[`localstack-init/01-criar-buckets.sh`](localstack-init/01-criar-buckets.sh):

```bash
docker compose exec localstack awslocal s3 ls
```

```
2026-09-24 02:44:28 athena-results
2026-09-24 02:44:28 vendas-curated
2026-09-24 02:44:28 vendas-raw
```

## Rodar

```bash
docker compose exec airflow-scheduler airflow dags test 10_s3_duckdb_local
```

```
enviado: s3://vendas-raw/vendas/vendas.csv
15 linha(s) descartada(s) na limpeza
gravado: s3://vendas-curated/vendas/vendas.parquet (841 linhas)

    categoria  pedidos    receita
  Eletronicos      196 3706991.59
   Mobiliario      200 2184226.23
Armazenamento      203  560574.93
  Perifericos      242  560563.88
```

Conferindo o que ficou no S3:

```bash
docker compose exec localstack awslocal s3 ls --recursive s3://vendas-raw/
docker compose exec localstack awslocal s3 ls --recursive s3://vendas-curated/
```

```
2026-09-24 02:49:54      45126 vendas/vendas.csv
2026-09-24 02:49:54      18639 vendas/vendas.parquet
```

## Como o Airflow sabe que é LocalStack

Por uma única variável no [`.env`](.env) — o `endpoint_url` é o que desvia o boto3:

```
AIRFLOW_CONN_AWS_DEFAULT={"conn_type":"aws","login":"test","password":"test","extra":{"region_name":"us-east-1","endpoint_url":"http://localstack:4566"}}
```

O DAG não sabe de nada disso: ele só pede `S3Hook(aws_conn_id="aws_default")`. Trocar de
backend é trocar essa linha — é o que a Parte B faz.

## Para ir além

A DAG 10 baixa o Parquet e roda o DuckDB sobre o arquivo local. Dá para consultar o S3
direto, sem baixar, com a extensão `httpfs`:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localstack:4566';
SET s3_use_ssl=false;
SET s3_url_style='path';
SELECT categoria, sum(receita) FROM read_parquet('s3://vendas-curated/vendas/vendas.parquet') GROUP BY 1;
```

Ficou de fora do DAG de propósito: o `INSTALL httpfs` baixa a extensão em tempo de execução
dentro do container, o que quebra sem internet e adiciona um ponto de falha ao exercício.

---

# Parte B — Passo a passo na AWS de verdade

O que vai ser criado: 1 usuário IAM, 1 role de serviço, 3 buckets, 1 Glue Database,
1 Glue Crawler. Tudo apagado no passo 10.

| Recurso | Custo do exercício |
|---|---|
| S3 (~64 KB, algumas horas) | ~US$ 0,00 |
| Glue Crawler (1 execução, mínimo de 10 min) | ~US$ 0,01 |
| Athena (1 query sobre ~19 KB; cobra por TB varrido, mínimo de 10 MB) | ~US$ 0,00 |
| **Total** | **< US$ 0,05** |

> **Atalho:** a pasta [`../terraform/`](../terraform/) cria tudo isso com um comando e gera
> o `.env.aws` pronto. Mas faça pelo menos uma vez na mão primeiro — os passos abaixo são
> o que o Terraform automatiza, e é bem mais fácil depurar algo que você já montou.

**Pré-requisito:** AWS CLI configurado com um usuário que possa criar IAM, S3 e Glue.

```bash
aws --version
aws sts get-caller-identity   # confirma a conta em que você está
```

Escolha um prefixo único (nomes de bucket são globais na AWS toda) e guarde o ID da conta:

```bash
export PREFIXO=andre-senai-s8
export CONTA=$(aws sts get-caller-identity --query Account --output text)
export REGIAO=us-east-1
```

> No PowerShell: `$env:PREFIXO="andre-senai-s8"` etc.

---

### 1. Criar os buckets

```bash
aws s3 mb s3://$PREFIXO-vendas-raw      --region $REGIAO
aws s3 mb s3://$PREFIXO-vendas-curated  --region $REGIAO
aws s3 mb s3://$PREFIXO-athena-results  --region $REGIAO
```

### 2. Criar o Glue Database

```bash
aws glue create-database --database-input "Name=vendas_db" --region $REGIAO
```

### 3. Criar a role de serviço do Glue

O crawler não usa as suas credenciais: ele assume uma role própria. **É aqui que a maioria
dos tutoriais quebra.**

```bash
cat > /tmp/glue-trust.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "glue.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
JSON

aws iam create-role \
  --role-name AWSGlueServiceRole-vendas \
  --assume-role-policy-document file:///tmp/glue-trust.json

aws iam attach-role-policy \
  --role-name AWSGlueServiceRole-vendas \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

A role gerenciada acima não dá acesso aos **seus** buckets. Falta isso:

```bash
cat > /tmp/glue-s3.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::$PREFIXO-vendas-curated",
      "arn:aws:s3:::$PREFIXO-vendas-curated/*"
    ]
  }]
}
JSON

aws iam put-role-policy \
  --role-name AWSGlueServiceRole-vendas \
  --policy-name LerBucketCurated \
  --policy-document file:///tmp/glue-s3.json
```

### 4. Criar o usuário IAM que o Airflow vai usar

```bash
aws iam create-user --user-name airflow-etl
```

Política mínima — repare no `iam:PassRole`, que é o que autoriza o Airflow a **entregar**
a role do passo 3 para o crawler:

```bash
cat > /tmp/airflow-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Buckets",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::$PREFIXO-vendas-raw",      "arn:aws:s3:::$PREFIXO-vendas-raw/*",
        "arn:aws:s3:::$PREFIXO-vendas-curated",  "arn:aws:s3:::$PREFIXO-vendas-curated/*",
        "arn:aws:s3:::$PREFIXO-athena-results",  "arn:aws:s3:::$PREFIXO-athena-results/*"
      ]
    },
    {
      "Sid": "Glue",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreateCrawler", "glue:UpdateCrawler", "glue:GetCrawler", "glue:StartCrawler"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Athena",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution", "athena:GetQueryExecution",
        "athena:GetQueryResults", "athena:GetWorkGroup"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EntregarRoleAoCrawler",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::$CONTA:role/AWSGlueServiceRole-vendas"
    }
  ]
}
JSON

aws iam put-user-policy \
  --user-name airflow-etl \
  --policy-name AirflowEtlVendas \
  --policy-document file:///tmp/airflow-policy.json
```

### 5. Gerar a access key

```bash
aws iam create-access-key --user-name airflow-etl
```

Anote `AccessKeyId` e `SecretAccessKey`. **O secret só aparece uma vez.**

### 6. Apontar o Airflow para a AWS

```bash
cp .env.aws.example .env.aws
```

Repare no nome: **`.env.aws`, não `.env`**. O `.env` é versionado (guarda o perfil
LocalStack, que só tem credencial falsa); o `.env.aws` está no `.gitignore` justamente
porque vai conter sua secret key.

Edite o `.env.aws` com os valores reais. O ponto crítico é a conexão: ela **não pode ter
`endpoint_url`**, senão o boto3 continua indo para o LocalStack.

```
AIRFLOW_CONN_AWS_DEFAULT={"conn_type":"aws","login":"AKIA...","password":"wJal...","extra":{"region_name":"us-east-1"}}

BUCKET_RAW=andre-senai-s8-vendas-raw
BUCKET_CURATED=andre-senai-s8-vendas-curated
BUCKET_ATHENA=andre-senai-s8-athena-results
GLUE_DATABASE=vendas_db
GLUE_CRAWLER=vendas-crawler
GLUE_ROLE=arn:aws:iam::123456789012:role/AWSGlueServiceRole-vendas
```

### 7. Subir sem o LocalStack

```bash
docker compose down
docker compose --env-file .env.aws up -d    # sem --profile local: o LocalStack nao sobe
docker compose ps                           # confirme que nao ha container "localstack"
```

`--env-file` troca o perfil sem editar nada versionado. Ele vem **antes** do subcomando.

### 8. Rodar o DAG

```bash
docker compose --env-file .env.aws exec airflow-scheduler airflow dags test 11_s3_glue_athena
```

O crawler leva de 1 a 3 minutos (a AWS cobra o mínimo de 10 minutos de DPU de qualquer jeito).
Saída esperada:

```
enviado: s3://andre-senai-s8-vendas-raw/vendas/vendas.csv
15 linha(s) descartada(s) na limpeza
gravado: s3://andre-senai-s8-vendas-curated/vendas/vendas.parquet (841 linhas)
...
    categoria  pedidos    receita
  Eletronicos      196 3706991.59
   Mobiliario      200 2184226.23
Armazenamento      203  560574.93
  Perifericos      242  560563.88
```

Os números têm que bater com os da DAG 10 — é o mesmo dado, a mesma consulta, só que
respondida pelo Athena em vez do DuckDB.

### 9. Conferir no console

```bash
# a tabela que o crawler descobriu sozinho
aws glue get-table --database-name vendas_db --name vendas --region $REGIAO \
  --query 'Table.StorageDescriptor.Columns'

# o resultado da query
aws s3 ls s3://$PREFIXO-athena-results/resultados/
```

Ou pelo console: **S3** → bucket curated · **AWS Glue** → Data Catalog → Tables ·
**Athena** → Query editor → Recent queries.

### 10. Destruir tudo

Não pule este passo. Bucket com objeto não pode ser removido, por isso o `rm` vem antes.

```bash
for b in $PREFIXO-vendas-raw $PREFIXO-vendas-curated $PREFIXO-athena-results; do
  aws s3 rm s3://$b --recursive
  aws s3 rb s3://$b
done

aws glue delete-crawler  --name vendas-crawler --region $REGIAO
aws glue delete-database --name vendas_db      --region $REGIAO

aws iam delete-role-policy --role-name AWSGlueServiceRole-vendas --policy-name LerBucketCurated
aws iam detach-role-policy --role-name AWSGlueServiceRole-vendas \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam delete-role --role-name AWSGlueServiceRole-vendas

aws iam delete-user-policy --user-name airflow-etl --policy-name AirflowEtlVendas
aws iam list-access-keys --user-name airflow-etl \
  --query 'AccessKeyMetadata[].AccessKeyId' --output text | \
  xargs -n1 aws iam delete-access-key --user-name airflow-etl --access-key-id
aws iam delete-user --user-name airflow-etl
```

Confirme que não sobrou nada: `aws s3 ls | grep $PREFIXO` não deve retornar linha nenhuma.

### 11. Voltar para o LocalStack

Basta parar de passar `--env-file`: o `.env` nunca foi tocado.

```bash
docker compose down
docker compose --profile local up -d
```

---

## E o MWAA?

O desafio cita MWAA como alternativa. A mesma DAG 11 roda lá sem alteração — o MWAA já
suporta **Airflow 3.3.1**, praticamente a mesma versão desta stack. Seria:

```bash
aws s3 cp dags/11_s3_glue_athena.py s3://<bucket-do-ambiente>/dags/
aws s3 cp requirements.txt          s3://<bucket-do-ambiente>/requirements.txt
```

**Mas o MWAA não tem free tier.** O menor ambiente (`mw1.small`) custa ~US$ 0,49/hora, mais
worker e NAT Gateway — cerca de **US$ 0,60/hora, ou ~US$ 430/mês se ficar ligado**. Um
ambiente esquecido de pé é a forma mais rápida de queimar crédito de estudante.

Por isso a trilha principal é o Airflow local apontando para a AWS real: o desafio pede
"rodar o DAG do S3/Glue/Athena", e é exatamente isso que a Parte B faz — pelo preço de um
café dividido por cem.

---

## Estrutura

```
airflow-aws/
├── docker-compose.yaml       # Airflow + Postgres + LocalStack (perfil "local")
├── Dockerfile                # imagem oficial + provider amazon, pandas, duckdb
├── requirements.txt
├── .env                      # perfil LocalStack (versionado)
├── .env.aws.example          # modelo do perfil AWS real (preenchido = NAO versionar)
├── localstack-init/
│   └── 01-criar-buckets.sh   # roda sozinho quando o LocalStack fica pronto
├── dags/
│   ├── 10_s3_duckdb_local.py
│   └── 11_s3_glue_athena.py
└── data/vendas.csv
```

---

## Se der problema

| Sintoma | Causa provável |
|---|---|
| `port is already allocated` | troque `AIRFLOW_PORT` no `.env` |
| `Could not connect to the endpoint URL` | LocalStack não subiu: faltou `--profile local` |
| DAG 11 dá `AccessDenied` no crawler | falta o `iam:PassRole` (passo 4) ou a policy de S3 na role (passo 3) |
| DAG 11 conecta no LocalStack sem querer | sobrou `endpoint_url` na conexão do `.env` |
| `EntityNotFoundException: Table vendas` | o crawler não achou nada: confira se o Parquet está em `s3://.../vendas/` |
| Athena `Insufficient permissions` no output | o workgroup `primary` precisa de output location, ou falta permissão no bucket de resultados |
| `NoSuchBucket` no LocalStack | o init script não rodou: `docker compose logs localstack \| grep buckets` |
| Crawler fica em `RUNNING` para sempre | normal até ~3 min; acima disso confira a role no console do Glue |

---

## Nota sobre segredos

O perfil LocalStack usa `test`/`test` — credenciais falsas, sem valor. O perfil AWS real
usa uma access key de verdade num arquivo local que **não vai para o git**.

Em produção nem isso: o Airflow puxaria a credencial do Secrets Manager, ou a instância
assumiria uma IAM role e não haveria access key nenhuma para vazar. No MWAA é assim por
padrão, via *execution role*.
