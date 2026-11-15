output "lake_bucket_id" {
  value = try(module.s3_lake[0].bucket_id, null)
}
