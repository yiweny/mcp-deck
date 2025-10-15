from sqlalchemy import create_engine
from .config import DATABASE_PATH


def get_engine():
    url = f"sqlite:///{DATABASE_PATH}"
    return create_engine(url, echo=False)


def get_connection():
    """Get a database connection from the engine."""
    engine = get_engine()
    return engine.connect()
