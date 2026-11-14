output "bucket_name" {
  description = "S3 data lake bucket name."
  value       = aws_s3_bucket.lake.bucket
}

output "bucket_arn" {
  description = "S3 data lake bucket ARN."
  value       = aws_s3_bucket.lake.arn
}

output "iam_user_name" {
  description = "IAM user name for pipeline access (create access keys manually)."
  value       = aws_iam_user.pipeline.name
}

output "data_root" {
  description = "DATA_ROOT value for .env after apply."
  value       = "s3://${aws_s3_bucket.lake.bucket}"
}