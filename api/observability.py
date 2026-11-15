"""Prometheus metrics shared by the API."""

from prometheus_client import Counter, Histogram

HTTP_REQUESTS = Counter(
    "finstream_api_requests_total",
    "API requests",
    ["method", "path", "status"],
)

HTTP_DURATION = Histogram(
    "finstream_api_request_duration_seconds",
    "API request duration",
    ["method", "path"],
)
