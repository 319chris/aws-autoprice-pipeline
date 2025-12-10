resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  suffix  = random_id.suffix.hex
  project = var.project
  env     = var.env
}

# --- S3 三个桶 ---

resource "aws_s3_bucket" "upload" {
  bucket        = "${local.project}-${local.env}-upload-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "curated" {
  bucket        = "${local.project}-${local.env}-curated-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "results" {
  bucket        = "${local.project}-${local.env}-results-${local.suffix}"
  force_destroy = true
}

# 禁止公开访问
resource "aws_s3_bucket_public_access_block" "upload" {
  bucket                  = aws_s3_bucket.upload.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 默认加密（SSE-S3）——注意嵌套 block 要分行
resource "aws_s3_bucket_server_side_encryption_configuration" "upload" {
  bucket = aws_s3_bucket.upload.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "results" {
  bucket = aws_s3_bucket.results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 预建 results/ 前缀（非必须，但有利于直观看 S3 结构）
resource "aws_s3_object" "results_prefix" {
  bucket  = aws_s3_bucket.results.id
  key     = "results/.keep"
  content = "keep"
}

# Athena WorkGroup，查询结果写入 results 桶
resource "aws_athena_workgroup" "wg" {
  name  = "${local.project}-${local.env}-wg"
  state = "ENABLED"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.results.bucket}/results/"
    }
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true
  }
}
