import os
import pandas as pd
from sqlalchemy import text
from .config import DATA_DIR
from .db import get_engine
import json
import yaml
from pathlib import Path


def load_schema(schema_path):
    if schema_path.suffix.lower() == ".json":
        return json.load(open(schema_path))
    elif schema_path.suffix.lower() in [".yml", ".yaml"]:
        return yaml.safe_load(open(schema_path))
    else:
        raise ValueError("Schema file must be .json or .yaml/.yml")


def generate_create_table_sql(schema):
    table_name = schema["table_name"]
    columns_sql = []
    for col in schema["columns"]:
        line = f"  {col['name']} {col['type']}"
        line += " NULL" if col.get("nullable", True) else " NOT NULL"
        columns_sql.append(line)

    if "primary_key" in schema:
        columns_sql.append(f"  PRIMARY KEY ({schema['primary_key']})")

    if "foreign_keys" in schema:
        for fk in schema["foreign_keys"]:
            ref = fk["references"]
            columns_sql.append(
                f"  FOREIGN KEY ({fk['column']}) REFERENCES {ref['table']}({ref['column']})"
            )

    columns_block = ",\n".join(columns_sql)
    sql = f"CREATE TABLE {table_name} (\n{columns_block}\n);"
    return sql


def load_parquet_to_db():
    """Load parquet files into database tables using schema definitions."""
    engine = get_engine()

    if not os.path.isdir(DATA_DIR):
        print(f"No data directory found at {DATA_DIR}")
        return

    # Define loading order: parent tables first, then child tables
    load_order = ["articles", "customers", "transactions"]

    # Schema directory
    schemas_dir = Path(__file__).parent.parent.parent / "schemas"

    # Drop tables in reverse dependency order
    drop_queries = [
        "DROP TABLE IF EXISTS transactions;",
        "DROP TABLE IF EXISTS customers;",
        "DROP TABLE IF EXISTS articles;",
    ]

    print("Dropping existing tables...")
    with engine.connect() as conn:
        for query in drop_queries:
            conn.execute(text(query))
            conn.commit()

    # Create tables using schema files
    print("Creating tables with foreign key constraints...")
    for table_name in load_order:
        schema_file = schemas_dir / f"{table_name}.json"
        if not schema_file.exists():
            print(
                f"Warning: Schema file {schema_file} not found, skipping table creation for {table_name}"
            )
            continue

        # Load schema and generate CREATE TABLE SQL
        schema = load_schema(schema_file)
        create_sql = generate_create_table_sql(schema)

        print(f"Creating table {table_name}...")
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()

    # Load data in the correct order
    for table_name in load_order:
        # Use clean articles data for articles table
        if table_name == "articles":
            filename = "articles_clean.parquet"
        else:
            filename = f"{table_name}.parquet"
        file_path = os.path.join(DATA_DIR, filename)

        if not os.path.exists(file_path):
            print(f"Warning: {file_path} not found, skipping...")
            continue

        print(f"Loading {file_path} into table {table_name}...")
        df = pd.read_parquet(file_path)

        # Insert data without dropping the table (append mode)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Loaded {len(df)} rows into {table_name}.")

    print("All data loaded successfully with foreign key relationships intact.")
