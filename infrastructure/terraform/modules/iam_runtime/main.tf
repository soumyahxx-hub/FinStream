data "aws_iam_policy_document" "lake_write" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [var.lake_bucket_arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${var.lake_bucket_arn}/*"]
  }
}

resource "aws_iam_policy" "lake_write" {
  count       = var.enabled ? 1 : 0
  name        = "${var.name_prefix}-lake-write"
  description = "Least-privilege S3 read/write for an explicitly deployed FinStream runtime."
  policy      = data.aws_iam_policy_document.lake_write.json
}
