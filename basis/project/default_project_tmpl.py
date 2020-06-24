metadata_storage = "sqlite:///.basis_metadata.db"  # Accepts any database url
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
    "basis_modules.common",
]
# add_default_python_runtime = True
# runtimes = []
