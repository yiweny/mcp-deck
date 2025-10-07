import os
import pandas as pd
from .config import DATA_DIR
from .db import get_engine

def load_parquet_to_mysql():
    """Load all .parquet files from the DATA_DIR into MySQL tables."""
    engine = get_engine()

    if not os.path.isdir(DATA_DIR):
        print(f"No data directory found at {DATA_DIR}")
        return

    for filename in os.listdir(DATA_DIR):
        if not filename.endswith(".parquet"):
            continue

        table_name = os.path.splitext(filename)[0]  # e.g. transactions.parquet -> transactions
        file_path = os.path.join(DATA_DIR, filename)

        print(f"Loading {file_path} into table {table_name}...")
        df = pd.read_parquet(file_path)

        # Write to MySQL (replace if table exists)
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"Loaded {len(df)} rows into {table_name}.")
