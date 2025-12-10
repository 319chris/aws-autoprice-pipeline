############################
# IAM Role for Lambda
############################

# 谁在运行（用于拼 account_id）
data "aws_caller_identity" "current" {}






############################
# Inline Policy (least-privilege)
############################


