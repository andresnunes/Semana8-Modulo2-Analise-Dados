# ---------------------------------------------------------------------------
# Infraestrutura da stack airflow-aws.
#
# Faz exatamente o que os passos 1 a 5 do README do airflow-aws fazem na mao,
# so que como codigo. Comparar os dois lado a lado e o objetivo do exercicio.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "atual" {}

locals {
  buckets = {
    raw     = "${var.prefixo}-vendas-raw"
    curated = "${var.prefixo}-vendas-curated"
    athena  = "${var.prefixo}-athena-results"
  }

  # O crawler le esta pasta; o nome dela vira o nome da tabela no Athena.
  prefixo_curated = "vendas"

  etiquetas = {
    Projeto = "senai-semana8"
    Origem  = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Passo 1 — Buckets
#
# force_destroy = true deixa o "terraform destroy" apagar buckets que ainda tem
# objeto dentro. Na AWS o padrao e recusar, e foi por isso que o teardown manual
# do README precisou de um "aws s3 rm --recursive" antes do "rb".
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "dados" {
  for_each = local.buckets

  bucket        = each.value
  force_destroy = true
  tags          = local.etiquetas
}

resource "aws_s3_bucket_public_access_block" "dados" {
  for_each = aws_s3_bucket.dados

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Passo 2 — Glue Data Catalog
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "vendas" {
  name        = var.glue_database
  description = "Catalogo das vendas curadas (criado pelo Terraform)"
}

# ---------------------------------------------------------------------------
# Passo 3 — Role que o crawler assume
#
# O crawler nao usa as credenciais do Airflow: ele assume uma role propria.
# Esquecer isso e o erro mais comum do exercicio.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_confianca" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "AWSGlueServiceRole-vendas"
  assume_role_policy = data.aws_iam_policy_document.glue_confianca.json
  tags               = local.etiquetas
}

resource "aws_iam_role_policy_attachment" "glue_gerenciada" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# A politica gerenciada acima nao da acesso aos SEUS buckets. Falta esta parte.
data "aws_iam_policy_document" "glue_le_curated" {
  statement {
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.dados["curated"].arn,
      "${aws_s3_bucket.dados["curated"].arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_le_curated" {
  name   = "LerBucketCurated"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_le_curated.json
}

resource "aws_glue_crawler" "vendas" {
  name          = var.glue_crawler
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.vendas.name
  tags          = local.etiquetas

  s3_target {
    path = "s3://${aws_s3_bucket.dados["curated"].id}/${local.prefixo_curated}/"
  }

  depends_on = [aws_iam_role_policy.glue_le_curated]
}

# ---------------------------------------------------------------------------
# Passos 4 e 5 — Usuario que o Airflow usa
# ---------------------------------------------------------------------------

resource "aws_iam_user" "airflow" {
  name          = var.usuario_airflow
  force_destroy = true
  tags          = local.etiquetas
}

data "aws_iam_policy_document" "airflow" {
  statement {
    sid     = "Buckets"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = flatten([
      for bucket in aws_s3_bucket.dados : [bucket.arn, "${bucket.arn}/*"]
    ])
  }

  statement {
    sid = "Glue"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions",
      "glue:CreateCrawler", "glue:UpdateCrawler", "glue:GetCrawler", "glue:StartCrawler",
    ]
    resources = ["*"]
  }

  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution",
      "athena:GetQueryResults", "athena:GetWorkGroup",
    ]
    resources = ["*"]
  }

  # Autoriza o Airflow a ENTREGAR a role do passo 3 para o crawler.
  # Sem isto o DAG falha com AccessDenied ao iniciar o crawler.
  statement {
    sid       = "EntregarRoleAoCrawler"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.glue.arn]
  }
}

resource "aws_iam_user_policy" "airflow" {
  name   = "AirflowEtlVendas"
  user   = aws_iam_user.airflow.name
  policy = data.aws_iam_policy_document.airflow.json
}

resource "aws_iam_access_key" "airflow" {
  user = aws_iam_user.airflow.name
}
