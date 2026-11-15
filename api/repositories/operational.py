"""Pooled, parameterized PostgreSQL reads for the FastAPI service."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any, cast

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class OperationalRepository:
    """Read-only operational repository backed by a bounded connection pool."""

    def __init__(
        self,
        database_url: str,
        min_pool_size: int = 1,
        max_pool_size: int = 10,
    ) -> None:
        if min_pool_size < 1 or max_pool_size < min_pool_size:
            raise ValueError("Invalid database connection pool bounds.")

        self._pool = ConnectionPool(
            conninfo=database_url,
            min_size=min_pool_size,
            max_size=max_pool_size,
            timeout=5,
            open=False,
            kwargs={"row_factory": dict_row},
        )

    def open(self) -> None:
        self._pool.open(wait=True, timeout=10)

    def close(self) -> None:
        self._pool.close()

    @contextmanager
    def _connection(self) -> Iterator[Connection[Any]]:
        with self._pool.connection() as connection:
            yield connection

    def healthcheck(self) -> None:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")

    def list_transactions(
        self,
        limit: int,
        offset: int = 0,
        *,
        source_system: str | None = None,
        transaction_type: str | None = None,
        currency: str | None = None,
        min_amount: Any | None = None,
        max_amount: Any | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor_position: tuple[datetime, str] | None = None,
    ) -> tuple[Sequence[dict[str, Any]], bool]:
        conditions: list[str] = []
        params: list[Any] = []

        filters = (
            ("source_system = %s", source_system),
            ("transaction_type = %s", transaction_type),
            ("currency = %s", currency),
            ("amount >= %s", min_amount),
            ("amount <= %s", max_amount),
            ("event_time >= %s", start_time),
            ("event_time < %s", end_time),
        )

        for condition, value in filters:
            if value is not None:
                conditions.append(condition)
                params.append(value)

        if cursor_position is not None:
            conditions.append("(event_time, event_id) < (%s, %s)")
            params.extend(cursor_position)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        statement = f"""
            SELECT event_id, source_system, source_record_id,
                   event_time, amount, currency, transaction_type
            FROM core.transactions
            {where_clause}
            ORDER BY event_time DESC, event_id DESC
            LIMIT %s OFFSET %s
        """
        params.extend((limit + 1, offset))

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statement, params)
            rows = cursor.fetchall()

        has_more = len(rows) > limit
        return rows[:limit], has_more

    def get_transaction(self, event_id: str) -> dict[str, Any] | None:
        statement = """
            SELECT event_id, source_system, source_record_id,
                   event_time, amount, currency, transaction_type
            FROM core.transactions
            WHERE event_id = %s
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statement, (event_id,))
            return cast(dict[str, Any] | None, cursor.fetchone())

    def list_alerts(
        self,
        limit: int,
        offset: int = 0,
        *,
        severity: str | None = None,
        rule_name: str | None = None,
        event_id: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor_position: tuple[datetime, str] | None = None,
    ) -> tuple[Sequence[dict[str, Any]], bool]:
        conditions: list[str] = []
        params: list[Any] = []

        filters = (
            ("severity = %s", severity),
            ("rule_name = %s", rule_name),
            ("event_id = %s", event_id),
            ("created_at >= %s", start_time),
            ("created_at < %s", end_time),
        )

        for condition, value in filters:
            if value is not None:
                conditions.append(condition)
                params.append(value)

        if cursor_position is not None:
            conditions.append("(created_at, alert_id) < (%s, %s)")
            params.extend(cursor_position)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        statement = f"""
            SELECT alert_id, event_id, created_at, rule_name,
                   risk_score, severity, explanation
            FROM core.fraud_alerts
            {where_clause}
            ORDER BY created_at DESC, alert_id DESC
            LIMIT %s OFFSET %s
        """
        params.extend((limit + 1, offset))

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statement, params)
            rows = cursor.fetchall()

        has_more = len(rows) > limit
        return rows[:limit], has_more

    def get_alert(self, alert_id: str) -> dict[str, Any] | None:
        statement = """
            SELECT alert_id, event_id, created_at, rule_name,
                   risk_score, severity, explanation
            FROM core.fraud_alerts
            WHERE alert_id = %s
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statement, (alert_id,))
            return cast(dict[str, Any] | None, cursor.fetchone())

    def summary(self) -> dict[str, Any]:
        statement = """
            SELECT
                (SELECT count(*) FROM core.transactions) AS transaction_count,
                (SELECT count(*) FROM core.fraud_alerts) AS fraud_alert_count,
                (SELECT max(event_time) FROM core.transactions) AS latest_event_time
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statement)
            row = cursor.fetchone()
            assert row is not None
            return cast(dict[str, Any], row)

    def customer_risk(self, customer_id: str) -> dict[str, Any]:
        statement = """
            SELECT
                count(DISTINCT t.event_id) AS transaction_count,
                count(a.alert_id) AS alert_count
            FROM core.transactions AS t
            LEFT JOIN core.fraud_alerts AS a ON a.event_id = t.event_id
            WHERE t.origin_account_id = %s
               OR t.destination_account_id = %s
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statement, (customer_id, customer_id))
            row = cursor.fetchone()
            assert row is not None
            return cast(dict[str, Any], row)

    def analytics_overview(self, start_time: datetime, end_time: datetime) -> dict[str, Any]:
        statement = """
            WITH transaction_stats AS (
                SELECT
                    count(*) AS transaction_count,
                    coalesce(sum(amount), 0) AS total_transaction_value,
                    avg(amount) AS average_transaction_value
                FROM core.transactions
                WHERE event_time >= %s AND event_time < %s
            ),
            alert_stats AS (
                SELECT
                    count(a.alert_id) AS alert_count,
                    count(DISTINCT a.event_id) AS alerted_transaction_count
                FROM core.transactions AS t
                JOIN core.fraud_alerts AS a ON a.event_id = t.event_id
                WHERE t.event_time >= %s AND t.event_time < %s
            )
            SELECT
                ts.transaction_count,
                ts.total_transaction_value,
                ts.average_transaction_value,
                als.alert_count,
                als.alerted_transaction_count,
                CASE
                    WHEN ts.transaction_count = 0 THEN NULL
                    ELSE round(
                        als.alerted_transaction_count::numeric * 100
                        / ts.transaction_count,
                        4
                    )
                END AS alert_rate_pct
            FROM transaction_stats AS ts
            CROSS JOIN alert_stats AS als
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                statement,
                (start_time, end_time, start_time, end_time),
            )
            row = cursor.fetchone()
            assert row is not None
            return cast(dict[str, Any], row)

    def transaction_trends(
        self,
        start_time: datetime,
        end_time: datetime,
        granularity: str,
    ) -> Sequence[dict[str, Any]]:
        if granularity not in {"hour", "day"}:
            raise ValueError("Granularity must be hour or day.")

        statement = """
            WITH transaction_buckets AS (
                SELECT
                    date_trunc(%s, event_time) AS bucket,
                    count(*) AS transaction_count,
                    sum(amount) AS total_transaction_value
                FROM core.transactions
                WHERE event_time >= %s AND event_time < %s
                GROUP BY 1
            ),
            alert_buckets AS (
                SELECT
                    date_trunc(%s, t.event_time) AS bucket,
                    count(a.alert_id) AS alert_count
                FROM core.transactions AS t
                JOIN core.fraud_alerts AS a ON a.event_id = t.event_id
                WHERE t.event_time >= %s AND t.event_time < %s
                GROUP BY 1
            )
            SELECT
                tx.bucket,
                tx.transaction_count,
                tx.total_transaction_value,
                coalesce(al.alert_count, 0) AS alert_count
            FROM transaction_buckets AS tx
            LEFT JOIN alert_buckets AS al USING (bucket)
            ORDER BY tx.bucket
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                statement,
                (
                    granularity,
                    start_time,
                    end_time,
                    granularity,
                    start_time,
                    end_time,
                ),
            )
            return cursor.fetchall()

    def analytics_distributions(self, start_time: datetime, end_time: datetime) -> dict[str, Any]:
        queries = {
            "source_systems": """
                SELECT
                    source_system AS category,
                    count(*) AS count,
                    sum(amount) AS total_value
                FROM core.transactions
                WHERE event_time >= %s AND event_time < %s
                GROUP BY source_system
                ORDER BY count DESC
            """,
            "transaction_types": """
                SELECT
                    coalesce(transaction_type, 'unknown') AS category,
                    count(*) AS count,
                    sum(amount) AS total_value
                FROM core.transactions
                WHERE event_time >= %s AND event_time < %s
                GROUP BY transaction_type
                ORDER BY count DESC
            """,
            "alert_severities": """
                SELECT
                    a.severity AS category,
                    count(*) AS count,
                    NULL::numeric AS total_value
                FROM core.fraud_alerts AS a
                JOIN core.transactions AS t ON t.event_id = a.event_id
                WHERE t.event_time >= %s AND t.event_time < %s
                GROUP BY a.severity
                ORDER BY count DESC
            """,
        }

        result: dict[str, Any] = {}
        with self._connection() as connection, connection.cursor() as cursor:
            for name, statement in queries.items():
                cursor.execute(statement, (start_time, end_time))
                result[name] = cursor.fetchall()

        return result

    def pipeline_operations(self) -> dict[str, Any]:
        processor_statement = """
            WITH latest_partition_status AS (
                SELECT DISTINCT ON (
                    processor_name, source_topic, partition_id
                )
                    processor_name,
                    source_topic,
                    partition_id,
                    offset_end,
                    processed_at,
                    status
                FROM core.processing_metadata
                ORDER BY
                    processor_name,
                    source_topic,
                    partition_id,
                    processed_at DESC
            )
            SELECT
                processor_name,
                source_topic,
                count(*) AS partition_count,
                count(*) FILTER (
                    WHERE status = 'completed'
                ) AS completed_partitions,
                count(*) FILTER (
                    WHERE status = 'failed'
                ) AS failed_partitions,
                max(processed_at) AS latest_processed_at,
                max(offset_end) AS latest_offset
            FROM latest_partition_status
            GROUP BY processor_name, source_topic
            ORDER BY processor_name, source_topic
        """

        reconciliation_statement = """
            SELECT
                r.run_id::text AS run_id,
                r.run_type,
                r.source_system,
                r.started_at,
                r.completed_at,
                r.status,
                count(rr.metric_name) AS metric_count,
                count(rr.metric_name) FILTER (
                    WHERE rr.expected_value <> rr.actual_value
                ) AS mismatch_count
            FROM core.reconciliation_runs AS r
            LEFT JOIN core.reconciliation_results AS rr
                ON rr.run_id = r.run_id
            GROUP BY r.run_id
            ORDER BY r.started_at DESC
            LIMIT 20
        """

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(processor_statement)
            processors = cursor.fetchall()

            cursor.execute(reconciliation_statement)
            reconciliations = cursor.fetchall()

        return {
            "processors": processors,
            "recent_reconciliations": reconciliations,
        }
