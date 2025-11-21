import os
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")

USER = os.getenv("WRDS_USER")
PASSWORD = os.getenv("WRDS_PASS")

sql_file_path = os.path.join(MODELS_DIR, "dl_funda_a.sql")

if not os.path.isfile(sql_file_path):
    print(f"❌ SQL file not found: {sql_file_path}")
else:
    with open(sql_file_path, "r") as f:
        sql_query = f.read()

    engine = create_engine(f"postgresql://{USER}:{PASSWORD}@wrds-pgdata.wharton.upenn.edu:9737/wrds?sslmode=require")
    
    try:
        with engine.connect() as conn:
            df_annual = pd.read_sql(sql_query, conn)
            print(df_annual.head())
    except ProgrammingError as e:
        # Detect missing column errors
        msg = str(e.orig)  # psycopg2 error message
        if "column" in msg and "does not exist" in msg:
            print(f"❌ SQL Error: {msg}")
        else:
            raise
    finally:
        engine.dispose()
