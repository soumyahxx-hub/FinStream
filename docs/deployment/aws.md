# AWS deployment guide

AWS is a later deployment target. Begin with encrypted private S3 only after an account, budget alarm, IAM role strategy, network design, data classification, and retention policy are approved. The foundation creates no EC2, MSK, NAT, managed database, or CloudWatch resource automatically.

For production, choose resilient Kafka/Spark deployment patterns, a secret provider, encrypted backups, IAM roles rather than static keys, TLS, private networking, centralized logs/metrics, and an incident response owner. Capture the resulting architecture in ADRs before implementation.

