output "lake_write_policy_arn" {
  value = try(aws_iam_policy.lake_write[0].arn, null)
}
