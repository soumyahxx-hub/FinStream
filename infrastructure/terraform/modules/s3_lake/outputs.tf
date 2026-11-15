output "bucket_id" {
  value       = try(aws_s3_bucket.this[0].id, null)
  description = "Bucket identifier when explicitly enabled."
}
