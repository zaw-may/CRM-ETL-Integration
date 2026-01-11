import logging
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

if not ENV_PATH.exists():
    raise FileNotFoundError(f".env file not found at {ENV_PATH}")

load_dotenv(ENV_PATH)

SQL_SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
DRIVER = os.getenv("DRIVER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

STAGING_TABLE = "stg_hubspot_deals"
FINAL_TABLE = "final_hubspot_deals"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# print("Connection Succed!")