"""
Postgres loader utilities for the NYC MTA dashboard.

Features
--------
- Single and composite-key UPSERT from a pandas DataFrame.
- Uses a temp staging table to avoid partial writes.
- Works with Neon (sslmode=require in your URL).
- Minimal dependencies: pandas, SQLAlchemy, python-dotenv.

Usage
-----
from src.load.to_postgres import upsert, get_engine
upsert(df, "fact_ridership_daily", pkey=["date", "mode"])  # composite PK
"""

from __future__ import annotations

import os
import uuid
from typing import Iterable, List, Optional, Sequence, Union

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load .env once (safe to call multiple times)
load_dotenv()


# ---------- Engine helpers ----------

def get_engine(env_var: str = "NEON_DATABASE_URL") -> Engine:
    """Create a SQLAlchemy engine from an env var (default: NEON_DATABASE_URL)."""
    url = os.getenv(env_var)
    if not url:
        raise RuntimeError(
            f"{env_var} is not set. Add it to your local .env or GitHub Action secret."
        )
    # pool_pre_ping avoids stale connections on serverless Postgres
    return create_engine(url, pool_pre_ping=True)


# ---------- Ident/SQL helpers ----------

def _quote_ident(name: str) -> str:
    """Very small identifier quoter (double quotes)."""
    return '"' + name.replace('"', '""') + '"'


def _normalize_pkey(pkey: Optional[Union[str, Sequence[str]]]) -> List[str]:
    if pkey is None:
        return []
    if isinstance(pkey, str):
        # comma- or space-separated
        parts = [p.strip() for p in pkey.replace(",", " ").split()]
        return [p for p in parts if p]
    return [str(p).strip() for p in pkey if str(p).strip()]


# ---------- Main API ----------

def upsert(
    df: pd.DataFrame,
    table: str,
    pkey: Optional[Union[str, Sequence[str]]] = None,
    schema: str = "public",
    engine: Optional[Engine] = None,
    chunksize: int = 10_000,
) -> int:
    """
    UPSERT dataframe into Postgres using a temp staging table.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to write. Column names must match target table column names.
    table : str
        Target table name in Postgres.
    pkey : str | list[str] | None
        Primary key columns for ON CONFLICT clause.
        - Example single key: "date"
        - Example composite: ["date", "mode"] or "date, mode"
        - If None, performs INSERT ... ON CONFLICT DO NOTHING (append-like).
    schema : str
        Target schema (default 'public').
    engine : sqlalchemy.Engine | None
        Optionally pass a prebuilt engine; otherwise constructed from NEON_DATABASE_URL.
    chunksize : int
        Chunk size for pandas to_sql into the staging table.

    Returns
    -------
    int
        Number of rows staged (not necessarily the number of rows updated).
    """
    if df is None or df.empty:
        return 0

    engine = engine or get_engine()
    cols = list(df.columns)
    if not cols:
        return 0

    # Build names
    schema_q = _quote_ident(schema)
    table_q = _quote_ident(table)
    tmp_table = f"_stg_{table}_{uuid.uuid4().hex[:8]}"
    tmp_q = _quote_ident(tmp_table)

    # Normalize PKs
    pkeys = _normalize_pkey(pkey)
    conflict_clause = ""
    update_clause = ""

    if pkeys:
        pkeys_q = ", ".join(_quote_ident(c) for c in pkeys)
        conflict_clause = f"ON CONFLICT ({pkeys_q}) "
        # Columns to update are all non-PK columns present in df
        update_cols = [c for c in cols if c not in pkeys]
        if update_cols:
            updates = ", ".join(f"{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}" for c in update_cols)
            update_clause = f"DO UPDATE SET {updates}"
        else:
            # If no non-PK cols, do nothing on conflict
            update_clause = "DO NOTHING"
    else:
        # No PKs specified — just avoid raising on duplicates
        conflict_clause = ""
        update_clause = ""

    # Quote the col list
    col_list = ", ".join(_quote_ident(c) for c in cols)

    with engine.begin() as conn:
        # 1) Stage into a temporary table in the target schema
        df.to_sql(
            tmp_table,
            con=conn,
            schema=schema,
            if_exists="fail",  # fail if collision (uuid prevents it)
            index=False,
            chunksize=chunksize,
            method="multi",
        )

        # 2) Build and execute the upsert from staging to target
        # INSERT INTO schema.table (cols) SELECT cols FROM schema.tmp
        # ON CONFLICT (pk...) DO UPDATE SET ...
        insert_sql = f"""
            INSERT INTO {schema_q}.{table_q} ({col_list})
            SELECT {col_list}
            FROM {schema_q}.{tmp_q}
        """
        if conflict_clause:
            insert_sql += f"\n{conflict_clause}{update_clause}"

        conn.exec_driver_sql(insert_sql)

        # 3) Drop the staging table
        conn.exec_driver_sql(f"DROP TABLE {schema_q}.{tmp_q}")

    return len(df)


def delete_where(
    table: str,
    where_sql: str,
    schema: str = "public",
    engine: Optional[Engine] = None,
) -> int:
    """
    Dangerous footgun (use carefully): DELETE rows matching a raw WHERE clause.

    Example:
    delete_where("fact_ridership_daily", "date >= current_date - interval '7 days'")
    """
    engine = engine or get_engine()
    schema_q = _quote_ident(schema)
    table_q = _quote_ident(table)
    sql = f"DELETE FROM {schema_q}.{table_q} WHERE {where_sql}"
    with engine.begin() as conn:
        res = conn.execute(text(sql))
        return res.rowcount or 0


def upsert_replace_recent_days(
    df: pd.DataFrame,
    table: str,
    date_col: str = "date",
    days: int = 7,
    pkey: Optional[Union[str, Sequence[str]]] = None,
    schema: str = "public",
    engine: Optional[Engine] = None,
) -> int:
    """
    Convenience for idempotent daily jobs:
      1) delete last N days of data (by date_col)
      2) upsert fresh rows

    Useful when upstream sources revise recent days.
    """
    if df is None or df.empty:
        return 0

    engine = engine or get_engine()
    with engine.begin() as conn:
        # Delete last N days
        conn.execute(text(f"""
            DELETE FROM {_quote_ident(schema)}.{_quote_ident(table)}
            WHERE { _quote_ident(date_col) } >= (current_date - interval '{int(days)} days')
        """))

    return upsert(df, table=table, pkey=pkey, schema=schema, engine=engine)
