#########################################################################
# Lambda 已回滚为“手工管理”：
# - 本文件不再声明任何 aws_lambda_* / archive_file / s3_bucket_notification
# - Terraform 仅管理 S3 三桶 + Athena WorkGroup（在 main.tf 里）
# - S3 事件通知请在控制台绑定到你要保留的函数（如 autoprice-fn/autoprice-dev-fn）
#########################################################################

# 提示：如果后续重新 IaC 化 Lambda：
# 1) 恢复 data "archive_file" + resource "aws_lambda_function"
# 2) 环境变量从 Terraform 输出注入（无需手填）
# 3) 用 aws_s3_bucket_notification 绑定 upload 桶，并保留唯一一份通知

# 占位：不创建任何资源，避免空文件被误删
locals {
  lambda_tf_placeholder = true
}