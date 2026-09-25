# Define as variáveis para o Terraforms utilizar

variable "prefixo" {
  description = "Prefixo dos nomes de bucket. Nomes de bucket sao globais na AWS inteira, entao use algo seu."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]{3,30}$", var.prefixo))
    error_message = "Use so minusculas, numeros e hifen (3 a 30 caracteres)."
  }
}

variable "regiao" {
  description = "Regiao da AWS onde tudo sera criado."
  type        = string
  default     = "us-east-1"
}

variable "glue_database" {
  description = "Nome do banco no Glue Data Catalog."
  type        = string
  default     = "vendas_db"
}

variable "glue_crawler" {
  description = "Nome do crawler que descobre o schema do Parquet."
  type        = string
  default     = "vendas-crawler"
}

variable "usuario_airflow" {
  description = "Usuario IAM que o Airflow usa para falar com a AWS."
  type        = string
  default     = "airflow-etl"
}
