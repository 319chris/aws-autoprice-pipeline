# 读取已经存在的 Lambda（我们暂时不创建，只读取它的 ARN/Name）
data "aws_lambda_function" "autoprice" {
  function_name = "autoprice-fn"   # 如果你的函数名不是这个，请改
}

# 允许新 upload 桶调用 Lambda（resource-based policy）
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "s3invoke-${local.env}"
  action        = "lambda:InvokeFunction"
  function_name = data.aws_lambda_function.autoprice.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.upload.arn
}

# 在 upload 桶上配置对象创建事件，触发 Lambda
resource "aws_s3_bucket_notification" "upload_events" {
  bucket = aws_s3_bucket.upload.id

  lambda_function {
    lambda_function_arn = data.aws_lambda_function.autoprice.arn
    events              = ["s3:ObjectCreated:*"]
    # 如需只监听某个前缀（比如 raw/），取消下面注释：
    # filter_prefix     = "raw/"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}
