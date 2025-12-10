terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

# 从 variables.tf 里的 var.region 取地区
provider "aws" {
  region  = var.region
  profile = "tf-admin" # ← 在这里指定你要用的 AWS CLI profile 名
}
