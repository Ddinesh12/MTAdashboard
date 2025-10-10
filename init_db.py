# init_db.py (repo root)
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os, re

load_dotenv()
engine = create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)

BASE = Path(__file__).parent.resolve()

def _statements_from_file(path: Path) -> list[str]:
    sql = Path(path).read_text(encoding="utf-8")

    # Strip single-line comments and inline "-- ..." safely
    cleaned_lines = []
    for line in sql.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)

    # Normalize whitespace then split on semicolons
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    stmts = [s.strip() for s in cleaned.split(";") if s.strip()]
    return stmts

def run_sql(path: Path) -> None:
    stmts = _statements_from_file(path)
    if not stmts:
        return
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))

if __name__ == "__main__":
    run_sql(BASE / "src" / "sql" / "schema.sql")
    run_sql(BASE / "src" / "sql" / "views.sql")
    print("DB initialized: tables + views are in place.")
