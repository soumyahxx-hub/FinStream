# Security guide

Never commit `.env`, credentials, raw public datasets, Terraform state, or screenshots containing sensitive information. Use environment variables locally and a managed secret mechanism later. Use IAM roles instead of long-lived cloud keys, block public S3 access, encrypt at rest/in transit, limit database roles, rotate secrets, and minimize identifiers in logs. Treat public-dataset fields as potentially sensitive even if synthetic or anonymized.

