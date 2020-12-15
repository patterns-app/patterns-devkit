metadata_storage = "sqlite:///.snapflow_metadata.db"  # Accepts any database url
storages = [
    # Add at least one storage
    # "postgresql://localhost:5432/db",
    # "mysql://localhost:3619/db",
    # "redshift://....",
    # "bigquery://....", # requires sqlalchemy-bigquery
    # "snowflake://....", # requires sqlalchemy-snowflake
    # "file:///usr/....",
    # "s3:///bucket/path", # Not supported yet
]
modules = [
    # "snapflow-stripe",
    # "snapflow-stocks",
]

### Setttings
# add_default_python_runtime = True
# cast_to_schema_level = "soft"
# warn_on_downcast = True
# fail_on_downcast = False
