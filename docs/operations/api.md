# API guide

Routes are GET /health, GET /transactions, GET /transactions/{transaction_id}, GET /fraud-alerts, GET /fraud-alerts/{alert_id}, GET /analytics/summary, and GET /customers/{customer_id}/risk. The service returns only parameterized PostgreSQL query results. A missing DB configuration returns 503; absent records return 404. Pagination uses validated limit and offset. Interactive OpenAPI documentation is at /docs when the API is running.

The customer risk route is an operational count summary, not a probability or an automated decision. Do not expose this foundation publicly without authentication, authorization, rate limits, audit logging, privacy review, and API versioning.

