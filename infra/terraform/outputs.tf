output "s3_upload_bucket" {
  value = aws_s3_bucket.upload.bucket
}
output "s3_curated_bucket" {
  value = aws_s3_bucket.curated.bucket
}
output "s3_results_bucket" {
  value = aws_s3_bucket.results.bucket
}
output "athena_workgroup_name" {
  value = aws_athena_workgroup.wg.name
}
output "athena_results_location" {
  value = "s3://${aws_s3_bucket.results.bucket}/results/"
}
