from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
e = create_engine(os.environ['NEON_DATABASE_URL'], pool_pre_ping=True)
with e.connect() as c:
    print(c.execute(text('select current_database(), current_user, now()')).fetchone())
