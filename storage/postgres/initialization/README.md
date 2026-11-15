# Initialization

Docker mounts `sql/ddl/001_core_schema.sql` into the official PostgreSQL image.
For an existing volume, apply reviewed migrations explicitly instead of relying
on container initialization behavior.
