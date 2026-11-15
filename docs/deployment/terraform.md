# Terraform guide

Terraform modules deliberately default to no resources: `deploy_enabled = false`. Copy the environment example tfvars, select a globally unique bucket name, run `terraform init -backend=false`, then `terraform validate` and `terraform plan`. Only set `deploy_enabled = true` after reviewing the plan, cost, IAM, naming, and bucket lifecycle. `prevent_destroy` is set on the S3 bucket, so destructive changes require explicit review. Never commit tfvars or state.

