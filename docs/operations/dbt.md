# dbt guide

Copy profiles.yml.example to a local profiles.yml or provide a secure profiles directory. Run dbt parse first, then dbt build only against a reviewed non-production database. Models stage canonical operational tables, build customer/activity intermediates, and materialize facts/dimensions/marts. The incremental models use event keys and ingestion timestamps; validate late-arrival handling before relying on them for financial reporting.

The snapshot is a foundation example. It must be reviewed after actual account/customer semantics and change tracking are profiled.

