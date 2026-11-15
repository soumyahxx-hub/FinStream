# Production intentionally inherits explicit opt-in behavior from dev until a reviewed network and IAM design exists.
module "s3_lake" {
  count       = var.deploy_enabled ? 1 : 0
  source      = "../../modules/s3_lake"
  enabled     = true
  bucket_name = var.lake_bucket_name
  tags        = var.tags
}
