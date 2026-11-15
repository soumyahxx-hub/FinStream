# Data quality strategy

Validate contracts before publication, reject explicable invalid records, and retain reason codes. dbt tests enforce key/null/relationship expectations in the warehouse. Reconciliation checks counts and IDs across a bounded run. Add source-specific rules only after profiling actual source columns; do not invent distribution thresholds or dataset statistics.

