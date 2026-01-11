import urllib
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from config import SQL_SERVER, DRIVER, DATABASE, SQL_USER, SQL_PASSWORD
import warnings

warnings.filterwarnings("ignore")

def get_engine():
    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )

    params = urllib.parse.quote_plus(odbc_str)

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        poolclass=StaticPool,     
        pool_pre_ping=True        
    )

    return engine

# def test_connection():
#     try:
#         engine = get_engine()
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT @@VERSION")).fetchone()
#             print("Connection successful!")
#             print("SQL Server version:", result[0])
#     except Exception as e:
#         print("Connection failed:")
#         print(e)


# if __name__ == "__main__":
#     test_connection()
