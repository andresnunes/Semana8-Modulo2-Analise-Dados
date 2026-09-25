# Terraform — infraestrutura da stack `airflow-aws`

Cria na AWS tudo que o DAG [`11_s3_glue_athena`](../airflow-aws/dags/11_s3_glue_athena.py)
precisa, com um comando em vez de dez.

É **o mesmo resultado** dos passos 1 a 5 da
[Parte B do README do airflow-aws](../airflow-aws/README.md#parte-b--passo-a-passo-na-aws-de-verdade).
A ideia é fazer uma vez na mão, para entender o que cada recurso é, e depois ver a mesma
coisa descrita como código.

## O que é criado

| Recurso | Para quê |
|---|---|
| 3 buckets S3 (`raw`, `curated`, `athena-results`) | dado bruto, Parquet limpo, resultado das queries |
| Glue Catalog Database | onde a tabela descoberta é registrada |
| Glue Crawler | lê o Parquet e descobre o schema sozinho |
| IAM Role `AWSGlueServiceRole-vendas` | identidade que o **crawler** assume |
| IAM User `airflow-etl` + policy + access key | identidade que o **Airflow** usa |

O Athena usa o workgroup `primary`, que já existe em toda conta — por isso não aparece aqui.

---

## Pré-requisitos

```bash
terraform version          # >= 1.6
aws sts get-caller-identity   # precisa poder criar IAM, S3 e Glue
```

---

## Usar

```bash
cd terraform

cp exemplo.tfvars meu.tfvars      # escolha um prefixo seu
terraform init
terraform plan  -var-file=meu.tfvars    # leia antes de aplicar
terraform apply -var-file=meu.tfvars
```

O `plan` mostra o que será criado sem criar nada. Vale ler a lista inteira na primeira vez:
é o inventário do que o exercício custa e do que precisa ser destruído depois.

### Gerando o `.env.aws`

O apply termina com um output que já é o arquivo de configuração pronto:

```bash
terraform output -raw env_aws > ../airflow-aws/.env.aws
```

E então, na pasta `airflow-aws`:

```bash
docker compose --env-file .env.aws up -d
docker compose --env-file .env.aws exec airflow-scheduler airflow dags test 11_s3_glue_athena
```

Volte para o [README do airflow-aws](../airflow-aws/README.md#8-rodar-o-dag) a partir do passo 8.

---

## Destruir

```bash
terraform destroy -var-file=meu.tfvars
```

Um comando apaga tudo, inclusive buckets com objeto dentro — os buckets têm
`force_destroy = true`. Foi por isso que o teardown manual do README precisou de um
`aws s3 rm --recursive` antes de cada `rb`: a AWS se recusa a apagar bucket não vazio,
e o Terraform faz a limpeza por você.

Depois confirme que não sobrou nada:

```bash
aws s3 ls | grep <seu-prefixo>
```

> Enquanto os buckets existirem o custo é praticamente zero (centavos por mês), mas o
> hábito de destruir o que você criou é o que evita susto na fatura.

---

## Dois pontos que valem atenção

### O state guarda a secret key

O `terraform.tfstate` registra tudo que foi criado, **incluindo a secret key do usuário IAM
em texto puro**. Por isso o [`.gitignore`](.gitignore) desta pasta bloqueia `*.tfstate*`.

Times de verdade resolvem isso guardando o state num bucket S3 com criptografia e
travamento, em vez de num arquivo local — é o chamado *remote backend*. Aqui ficou local
de propósito, para o exercício ter uma peça a menos.

### `iam:PassRole` é a pegadinha

Repare no último `statement` de `data.aws_iam_policy_document.airflow` em
[`main.tf`](main.tf). O Airflow não só precisa poder iniciar o crawler: ele precisa de
permissão para **entregar** a role do Glue para ele. Sem esse `iam:PassRole` o DAG falha
com `AccessDenied` numa mensagem que não explica muito.

---

## Estrutura

```
terraform/
├── versions.tf     # versao do Terraform e do provider AWS
├── variables.tf    # entradas (prefixo, regiao, nomes)
├── main.tf         # os recursos
├── outputs.tf      # saidas, incluindo o .env.aws pronto
└── exemplo.tfvars  # modelo de configuracao
```

Separar em quatro arquivos é convenção, não obrigação: o Terraform lê todos os `.tf` da
pasta como se fossem um só. A divisão existe para humanos acharem as coisas.
