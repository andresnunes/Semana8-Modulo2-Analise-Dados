output "conta" {
  description = "ID da conta AWS em que tudo foi criado."
  value       = data.aws_caller_identity.atual.account_id
}

output "buckets" {
  description = "Buckets criados."
  value       = { for nome, bucket in aws_s3_bucket.dados : nome => bucket.id }
}

output "glue_role_arn" {
  description = "ARN da role que o crawler assume."
  value       = aws_iam_role.glue.arn
}

# O arquivo .env.aws pronto para a stack airflow-aws.
#
# Marcado como sensitive porque carrega a secret key: o Terraform esconde o
# valor na tela, e so mostra com o comando do README.
output "env_aws" {
  description = "Conteudo do airflow-aws/.env.aws. Veja o README para extrair."
  sensitive   = true
  value       = <<-EOT
    AIRFLOW_UID=50000
    AIRFLOW_PORT=8082

    AIRFLOW_CONN_AWS_DEFAULT={"conn_type":"aws","login":"${aws_iam_access_key.airflow.id}","password":"${aws_iam_access_key.airflow.secret}","extra":{"region_name":"${var.regiao}"}}

    BUCKET_RAW=${aws_s3_bucket.dados["raw"].id}
    BUCKET_CURATED=${aws_s3_bucket.dados["curated"].id}
    BUCKET_ATHENA=${aws_s3_bucket.dados["athena"].id}
    GLUE_DATABASE=${aws_glue_catalog_database.vendas.name}
    GLUE_CRAWLER=${aws_glue_crawler.vendas.name}
    GLUE_ROLE=${aws_iam_role.glue.arn}
  EOT
}
