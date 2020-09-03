metadata_storage = "sqlite:///.dags_metadata.db"  # Accepts any database url
storages = [
    # Add at least one storage
    # "postgres://localhost:5432/db",
    # "mysql://localhost:3619/db",
    # "redshift://....",
    # "bigquery://....",
    # "file:///usr/....",
    # "s3:///bucket/path",
]
modules = [
    "dags_modules.common",
]
# add_default_python_runtime = True
# runtimes = []
