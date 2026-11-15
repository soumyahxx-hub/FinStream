variable "deploy_enabled" { type = bool, default = false }
variable "aws_region" { type = string, default = "ap-south-1" }
variable "lake_bucket_name" { type = string, default = "" }
variable "tags" {
  type    = map(string)
  default = { Project = "FinStream", Environment = "prod", ManagedBy = "Terraform" }
}
