variable "enabled" {
  type        = bool
  description = "Explicit opt-in. Defaults are configured in each environment."
}
variable "bucket_name" {
  type        = string
  description = "Globally unique bucket name supplied by the deployer."
}
variable "tags" {
  type        = map(string)
  description = "Resource tags required by the environment."
  default     = {}
}
