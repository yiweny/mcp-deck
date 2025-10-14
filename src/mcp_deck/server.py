from fastmcp import FastMCP
from mcp_deck.loader import load_parquet_to_mysql
from mcp_deck.tools.tools import (
    get_user_purchase_history,
    get_details_of_list_of_products,
)

app = FastMCP("MCP Deck", version="0.1")


@app.tool()
def user_purchase_history(user_id: str):
    """Expose the get_user_purchase_history tool."""
    return get_user_purchase_history(user_id)


@app.tool()
def product_details(product_ids: list[int]):
    """Expose the get_details_of_list_of_products tool."""
    return get_details_of_list_of_products(product_ids)


if __name__ == "__main__":
    # Run any initialization code BEFORE launching the server
    print("Initializing DB and loading data...")
    load_parquet_to_mysql()

    # Run FastMCP with SSE transport for HTTP access
    app.run(transport="sse", host="0.0.0.0", port=8080)
