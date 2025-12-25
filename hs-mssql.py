"""
End-to-end HubSpot JSON -> SQL Server loader

Steps:
1. Load HubSpot JSON
2. Normalize & clean data
3. Load into staging table
4. MERGE into final table
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import logging
from hubspot import HubSpot
from hubspot.crm.deals import ApiException

# CONFIGURATION

JSON_FILE_PATH = "jsonoutput.json"

SQL_SERVER = "CE-BKK-LPT-067" 
DATABASE = "ce-hs-dev" 
USERNAME = "sa" 
PASSWORD = "password"


# Private App or OAuth Token (From App Distribution)
ACCESS_TOKEN = "pat-na2-xxxxx"  

STAGING_TABLE = "dbo.stg_hubspot_deals"
FINAL_TABLE = "dbo.hubspot_deals"

try:
    client = HubSpot(access_token=ACCESS_TOKEN)
except Exception as e:
    print(f"Error initializing HubSpot client: {e}")
    exit()

# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# SQL CONNECTION

def get_engine():
    conn_str = (
        f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SQL_SERVER}/{DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(conn_str, fast_executemany=True)

# LOAD JSON

def load_hubspot_json() -> pd.DataFrame:
    logging.info("Loading HubSpot JSON")
    def get_all_deals():
        """
        Fetches all deals from HubSpot using the API client, handling pagination.
        """
        all_deals = []
        after = None
        limit = 100 

        while True:
            try:
                # Get a page of deals, requesting specific properties if needed
                api_response = client.crm.deals.basic_api.get_page(
                    limit=limit,
                    after=after,
                    archived=False,
                    properties=["dealname", "amount", "dealstage", "closedate", "pipeline", "description", "dealtype", "createdate", "days_to_close"]
                )
            
                all_deals.extend(api_response.results)
            
                # Check for the next page
                if api_response.paging and api_response.paging.next:
                    after = api_response.paging.next.after
                else:
                    break # No more pages
                
            except ApiException as e:
                print(f"Exception when calling basic_api->get_page: {e}\n")
                break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break
            
        return all_deals

    # Fetch the data
    deals_data = get_all_deals()
    deals_details = []

    # Collect the data 
    if deals_data:
        deals_list = list(deals_data) 
    
        for dd in deals_list:
            deals_details.append({
                "archived": dd.archived,
                "archived_at": dd.archived_at,
                "associations": dd.associations,
                "created_at": dd.created_at,
                "id":dd.id,
                "object_write_trace_id": dd.object_write_trace_id,
                "properties": dd.properties,
                "properties_with_history": dd.properties_with_history,
                "updated_at": dd.updated_at            
            })     

    # Create a Pandas DataFrame for easy analysis
    df = pd.DataFrame(deals_details)
    logging.info(f"Loaded {len(df)} records")

    return df

# CLEAN DATA

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning data")

    # Ensure required columns exist
    required_columns = [
        "archived", "archived_at", "associations", "created_at", 
        "id", "object_write_trace_id", "properties", "properties_with_history", "updated_at",
        "amount", "closedate", "createdate", "days_to_close", "dealname", "dealstage", "dealtype",
        "description", "hs_lastmodifieddate", "hs_object_id", "pipeline"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Type conversions
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["createdate"] = pd.to_datetime(df["createdate"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    return df[required_columns]

# LOAD TO STAGING TABLE

def load_to_staging(df: pd.DataFrame, engine):
    logging.info("Loading data into staging table")

    df.to_sql(
        name=STAGING_TABLE.split(".")[1],
        schema="dbo",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    
    logging.info("Staging load completed")

# MERGE INTO FINAL TABLE

def merge_to_final(engine):
    logging.info("Merging staging into final table")

    merge_sql = f"""
    MERGE {FINAL_TABLE} AS tgt
    USING {STAGING_TABLE} AS src
        ON tgt.id = src.id

    WHEN MATCHED AND src.updatedAt > tgt.updatedAt THEN
        UPDATE SET
            tgt.dealname = src.dealname,
            tgt.amount = src.amount,
            tgt.pipeline = src.pipeline,
            tgt.updatedAt = src.updatedAt

    WHEN NOT MATCHED THEN
        INSERT (id, dealname, amount, pipeline, updatedAt)
        VALUES (src.id, src.dealname, src.amount, src.pipeline, src.updatedAt);
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    logging.info("Merge completed")

# MAIN

def main():
    logging.info("HubSpot JSON -> SQL Server ETL started")

    if not Path(JSON_FILE_PATH).exists():
        raise FileNotFoundError("JSON file not found")

    engine = get_engine()

    df = load_hubspot_json()
    df = clean_dataframe(df)
    # load_to_staging(df, engine)
    # merge_to_final(engine)

    logging.info("ETL completed successfully")

if __name__ == "__main__":
    main()