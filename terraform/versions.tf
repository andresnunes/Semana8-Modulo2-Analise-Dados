# vesões que esse código do terraform usa
# Versão to Terraform em si
# Versão do plugin da AWS no TF
terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.regiao
}
