import os
import os.path as osp
from dotenv import load_dotenv

load_dotenv()

# SQLite database path
DATABASE_PATH = os.getenv(
    "DATABASE_PATH", osp.join(osp.dirname(__file__), "..", "..", "..", "mcp_deck.db")
)

DATA_DIR = osp.join(osp.dirname(__file__), "..", "..", "data")
