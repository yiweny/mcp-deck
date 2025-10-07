import pandas as pd
import pyarrow.parquet as pq
import json
import yaml
from pathlib import Path
import argparse

MYSQL_TYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "DOUBLE",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "string": "VARCHAR(255)",
    "object": "TEXT",
    "datetime64[ns]": "DATETIME",
}


def infer_mysql_type(dtype):
    dtype = str(dtype)
    for k, v in MYSQL_TYPE_MAP.items():
        if k in dtype:
            return v
    return "TEXT"


def generate_schema(parquet_path, table_name=None, pkey=None, fkeys=None):
    parquet_file = pq.read_table(parquet_path)
    df = parquet_file.to_pandas()
    table_name = table_name or Path(parquet_path).stem

    columns = []
    for col in df.columns:
        dtype = infer_mysql_type(df[col].dtype)
        nullable = df[col].isnull().any()
        columns.append({"name": col, "type": dtype, "nullable": bool(nullable)})

    schema = {
        "table_name": table_name,
        "columns": columns,
    }

    if pkey:
        schema["primary_key"] = pkey

    if fkeys:
        schema["foreign_keys"] = fkeys

    return schema


def save_schema(schema, output_path):
    if output_path.suffix.lower() == ".json":
        with open(output_path, "w") as f:
            json.dump(schema, f, indent=2)
    elif output_path.suffix.lower() in [".yml", ".yaml"]:
        with open(output_path, "w") as f:
            yaml.dump(schema, f, sort_keys=False)
    else:
        raise ValueError("Output file must end with .json or .yaml/.yml")


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
    sql = (
        f"CREATE TABLE {table_name} (\n"
        f"{columns_block}\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )
    return sql


def save_sql(sql, output_path):
    with open(output_path, "w") as f:
        f.write(sql)
        f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate MySQL schema or SQL from Parquet or schema file"
    )
    parser.add_argument("--input", type=Path, help="Path to Parquet file")
    parser.add_argument(
        "--output", type=Path, help="Output schema file (.yaml or .json)"
    )
    parser.add_argument("--table", type=str, help="Table name override")
    parser.add_argument("--pkey", type=str, help="Primary key column name")
    parser.add_argument(
        "--fkeys", type=str, nargs="*", help="Foreign key spec as column=table.column"
    )

    parser.add_argument(
        "--from-schema", type=Path, help="Path to existing YAML/JSON schema"
    )
    parser.add_argument("--sql-out", type=Path, help="Output .sql file")

    args = parser.parse_args()

    if args.input and args.output:
        # Step 1: Generate schema from parquet
        fkeys = []
        if args.fkeys:
            for fk in args.fkeys:
                col, ref = fk.split("=")
                ref_table, ref_col = ref.split(".")
                fkeys.append(
                    {
                        "column": col,
                        "references": {"table": ref_table, "column": ref_col},
                    }
                )
        schema = generate_schema(args.input, args.table, args.pkey, fkeys)
        save_schema(schema, args.output)
        print(f"✅ Schema saved to {args.output}")

    if args.from_schema and args.sql_out:
        # Step 2: Generate CREATE TABLE SQL from schema
        schema = load_schema(args.from_schema)
        sql = generate_create_table_sql(schema)
        save_sql(sql, args.sql_out)
        print(f"✅ SQL saved to {args.sql_out}")
