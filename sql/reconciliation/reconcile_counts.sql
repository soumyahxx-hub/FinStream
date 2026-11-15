-- Parameterize :run_id and compare source manifest counts with sink unique event IDs.
SELECT metric_name, expected_value, actual_value, expected_value - actual_value AS difference
FROM core.reconciliation_results
WHERE run_id = :run_id
ORDER BY metric_name;
