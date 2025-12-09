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

# Load variables from .env into the environment (safe to call multiple times)
load_dotenv()


# ---------- Engine helpers ----------

def get_engine(env_var: str = "NEON_DATABASE_URL") -> Engine:
    """
    Create a SQLAlchemy engine from a database URL stored in an env var.

    - By default, it reads NEON_DATABASE_URL (for Neon Postgres).
    - Raises a clear error if the variable is not set.
    - Uses `pool_pre_ping=True` to avoid stale connections in serverless setups.
    """
    url = os.getenv(env_var)
    if not url:
        raise RuntimeError(
            f"{env_var} is not set. Add it to your local .env or GitHub Action secret."
        )
    # pool_pre_ping sends a lightweight check before reusing a connection
    return create_engine(url, pool_pre_ping=True)


# ---------- Ident/SQL helpers ----------

def _quote_ident(name: str) -> str:
    """
    Very small identifier quoter.

    - Wraps a name in double quotes.
    - Escapes any internal double quotes by doubling them.
    - This is enough for simple column and table names.
    """
    return '"' + name.replace('"', '""') + '"'


def _normalize_pkey(pkey: Optional[Union[str, Sequence[str]]]) -> List[str]:
    """
    Normalize the pkey argument into a list of column names (strings).

    - Accepts:
        None            -> []
        "date"          -> ["date"]
        "date, mode"    -> ["date", "mode"]
        ["date","mode"] -> ["date", "mode"]
    - Trims whitespace and ignores empty segments.
    """
    if pkey is None:
        return []
    if isinstance(pkey, str):
        # Allow comma- or space-separated specification
        parts = [p.strip() for p in pkey.replace(",", " ").split()]
        return [p for p in parts if p]
    # Sequence-like pkey, convert each element to a stripped string
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
    UPSERT a DataFrame into Postgres using a temporary staging table.

    Steps:
      1) Create a temp table with the same columns as the DataFrame.
      2) Bulk-load the DataFrame into the temp table via pandas.to_sql.
      3) INSERT from temp into the target table with an ON CONFLICT clause:
         - If pkey provided: ON CONFLICT (pkey...) DO UPDATE ...
         - If pkey missing: pure INSERT (no conflict handling).
      4) Drop the temp table.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to write. Column names must match target table column names.
    table : str
        Target table name in Postgres.
    pkey : str | list[str] | None
        Primary key column(s) for the ON CONFLICT clause.
        - Example single key: "date"
        - Example composite: ["date", "mode"] or "date, mode"
        - If None, we do not add ON CONFLICT; duplicates will error if PK exists.
    schema : str
        Target schema (default 'public').
    engine : sqlalchemy.Engine | None
        Optional pre-built engine; if None, we call get_engine().
    chunksize : int
        Chunk size for pandas.to_sql when loading the staging table.

    Returns
    -------
    int
        Number of rows taken from the DataFrame (len(df)).
        (This is the number staged, not the number actually inserted/updated.)
    """
    # If there's nothing to write, return early
    if df is None or df.empty:
        return 0

    # Use provided engine or build one off NEON_DATABASE_URL
    engine = engine or get_engine()
    cols = list(df.columns)
    if not cols:
        return 0

    # Quote schema/table names and build a unique temp table name
    schema_q = _quote_ident(schema)
    table_q = _quote_ident(table)
    tmp_table = f"_stg_{table}_{uuid.uuid4().hex[:8]}"
    tmp_q = _quote_ident(tmp_table)

    # Normalize primary-key list and build conflict/update clauses
    pkeys = _normalize_pkey(pkey)
    conflict_clause = ""
    update_clause = ""

    if pkeys:
        # Build ON CONFLICT (pk1, pk2, ...)
        pkeys_q = ", ".join(_quote_ident(c) for c in pkeys)
        conflict_clause = f"ON CONFLICT ({pkeys_q}) "
        # Columns to update are all non-PK columns found in df
        update_cols = [c for c in cols if c not in pkeys]
        if update_cols:
            # Build DO UPDATE SET col = EXCLUDED.col for each non-PK column
            updates = ", ".join(
                f"{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}" for c in update_cols
            )
            update_clause = f"DO UPDATE SET {updates}"
        else:
            # Edge case: no non-PK columns to update -> no-op on conflict
            update_clause = "DO NOTHING"
    else:
        # If no pkey is given, we don't attach any ON CONFLICT clause
        conflict_clause = ""
        update_clause = ""

    # Quote the column list for both staging and target table usage
    col_list = ", ".join(_quote_ident(c) for c in cols)

    # Use a transaction context so staging + insert + drop is atomic
    with engine.begin() as conn:
        # 1) Stage data into a temporary table in the target schema.
        #    The temp table name is random to avoid collisions.
        df.to_sql(
            tmp_table,
            con=conn,
            schema=schema,
            if_exists="fail",  # fail if name collision (uuid makes this very unlikely)
            index=False,
            chunksize=chunksize,
            method="multi",
        )

        # 2) Construct the INSERT ... SELECT ... statement from staging to target
        insert_sql = f"""
            INSERT INTO {schema_q}.{table_q} ({col_list})
            SELECT {col_list}
            FROM {schema_q}.{tmp_q}
        """
        # Attach ON CONFLICT clause if we have one
        if conflict_clause:
            insert_sql += f"\n{conflict_clause}{update_clause}"

        # Execute the upsert
        conn.exec_driver_sql(insert_sql)

        # 3) Drop the staging table to clean up
        conn.exec_driver_sql(f"DROP TABLE {schema_q}.{tmp_q}")

    # Return number of rows in the original DataFrame
    return len(df)


def delete_where(
    table: str,
    where_sql: str,
    schema: str = "public",
    engine: Optional[Engine] = None,
) -> int:
    """
    Delete rows from a table using a raw WHERE clause.

    Parameters
    ----------
    table : str
        Target table name in Postgres.
    where_sql : str
        Raw SQL WHERE condition (without the 'WHERE' keyword).
        Example: "date >= current_date - interval '7 days'".
    schema : str
        Schema containing the table (default 'public').
    engine : sqlalchemy.Engine | None
        Optional pre-built engine.

    Returns
    -------
    int
        Number of rows deleted.

    Note
    ----
    This is intentionally low-level and can be dangerous if misused,
    since it accepts raw SQL.
    """
    engine = engine or get_engine()
    schema_q = _quote_ident(schema)
    table_q = _quote_ident(table)
    sql = f"DELETE FROM {schema_q}.{table_q} WHERE {where_sql}"
    with engine.begin() as conn:
        res = conn.execute(text(sql))
        # rowcount may be None; fallback to 0
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
    Convenience wrapper for idempotent daily/periodic jobs.

    Pattern:
      1) Delete the last N days of rows (based on `date_col`).
      2) Upsert fresh rows for that period from `df`.

    This is useful when upstream sources revise recent data, and we want
    to simply "replace last N days" on each run instead of dealing with
    per-row diffing.

    Parameters
    ----------
    df : pandas.DataFrame
        New data for at least the last `days` days.
    table : str
        Target table in Postgres.
    date_col : str
        Name of the date column in the table (default 'date').
    days : int
        How many days back from current_date to delete before upserting.
    pkey : str | list[str] | None
        Primary key definition to pass through to `upsert`.
    schema : str
        Schema name (default 'public').
    engine : sqlalchemy.Engine | None
        Optional engine; otherwise a new one is created.

    Returns
    -------
    int
        Number of rows from df passed through to the underlying `upsert`.
    """
    if df is None or df.empty:
        return 0

    engine = engine or get_engine()

    # Delete the last `days` worth of data based on date_col
    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM {_quote_ident(schema)}.{_quote_ident(table)}
            WHERE { _quote_ident(date_col) } >= (current_date - interval '{int(days)} days')
        """))

    # Insert the fresh rows for that window via standard upsert
    return upsert(df, table=table, pkey=pkey, schema=schema, engine=engine)
