from mcp_deck.db import get_engine

def query_db(query: str) -> list[dict]:
    """Execute a SQL SELECT query and return results as a list of dicts."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def get_user_purchase_history(user_id: str) -> list[dict]:
    """Get the purchase history of a user sorted by date."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM transactions WHERE user_id = %s ORDER BY date ASC", (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def get_details_of_list_of_products(product_ids: list[str]) -> list[dict]:
    """Get the details of a list of products."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM articles WHERE id IN (%s)", (product_ids,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows