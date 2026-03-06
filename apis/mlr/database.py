import duckdb
from contextlib import contextmanager
from .config import MOTHERDUCK_DSN


@contextmanager
def get_db():
    """Yield a DuckDB connection to MotherDuck."""
    con = duckdb.connect(MOTHERDUCK_DSN)
    try:
        yield con
    finally:
        con.close()
