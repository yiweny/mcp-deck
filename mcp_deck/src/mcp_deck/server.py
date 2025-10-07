from fastmcp import FastMCP
from .db import get_engine
from .loader import load_parquet_to_mysql
from .tools.tools import query_db

app = FastMCP("MCP Skeleton Server with Data", version="0.2")

@app.on_startup
def startup():
    print("Initializing MySQL and loading data...")
    load_parquet_to_mysql()

# Register tools
app.register_tool(query_db)

if __name__ == "__main__":
    app.run()