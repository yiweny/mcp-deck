from sqlalchemy import create_engine
from .config import MYSQL_CONFIG


def get_engine():
    url = (
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
        f"@{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}"
    )
    return create_engine(url, echo=False)


def get_connection():
    """Get a database connection from the engine."""
    engine = get_engine()
    return engine.connect()
