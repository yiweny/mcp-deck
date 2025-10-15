from ..db import get_connection


def query_db(query: str) -> list[dict]:
    """Execute a SQL SELECT query and return results as a list of dicts."""
    from sqlalchemy import text

    conn = get_connection()
    result = conn.execute(text(query))
    rows = result.fetchall()
    # Convert to dict format for consistency
    columns = result.keys()
    result_list = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return result_list


def get_user_purchase_history(user_id: str) -> list[dict]:
    """Get the purchase history of a user sorted by date."""
    from sqlalchemy import text

    conn = get_connection()
    result = conn.execute(
        text(
            "SELECT * FROM transactions WHERE customer_id = :user_id ORDER BY t_dat ASC"
        ),
        {"user_id": user_id},
    )
    rows = result.fetchall()
    # Convert to dict format for consistency
    columns = result.keys()
    result_list = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return result_list


def get_details_of_list_of_products(product_ids: list[str]) -> list[dict]:
    """Get the details of a list of products."""
    from sqlalchemy import text

    conn = get_connection()
    # Create placeholders for SQLAlchemy
    placeholders = ",".join(f":id{i}" for i in range(len(product_ids)))
    params = {f"id{i}": pid for i, pid in enumerate(product_ids)}
    result = conn.execute(
        text(f"SELECT * FROM articles WHERE article_id IN ({placeholders})"), params
    )
    rows = result.fetchall()
    # Convert to dict format for consistency
    columns = result.keys()
    result_list = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return result_list
