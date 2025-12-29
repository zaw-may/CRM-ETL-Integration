"""
Steps:
1. Load HubSpot records
2. Normalize & clean data
3. Load into staging table
4. MERGE into final table
"""

import json
import pandas as pd
from sqlalchemy import Table, MetaData, Column, create_engine, insert, text, DateTime, Float, String, Integer
from pathlib import Path
import logging
from hubspot import HubSpot
from hubspot.crm.deals import ApiException
import urllib
from sqlalchemy.pool import StaticPool
import warnings

# WARNING [Optional]

warnings.filterwarnings("ignore")

# CONFIGURATION

SQL_SERVER = "SERVER\INSTANCE" 
DATABASE = "mydb" 
USERNAME = "uid" 
PASSWORD = "pwd"
DRIVER = "ODBC Driver 17 for SQL Server"

# Private App or OAuth Token (From App Distribution)

ACCESS_TOKEN = "pat-na2-xxxxxxx"  

STAGING_TABLE = "stg_hubspot_deals"
FINAL_TABLE = "hubspot_deals"

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
    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
    )

    params = urllib.parse.quote_plus(odbc_str)

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        poolclass=StaticPool,     # prevents multi-connection issues
        pool_pre_ping=True        # checks connection health
    )

    return engine

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
    flatten_df = pd.json_normalize(deals_details)
    logging.info(f"Loaded {len(df)} records")

    return flatten_df

# CLEAN DATA

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning data")

    # Ensure required columns exist
    required_columns = [
        "archived", "archived_at", "associations", "created_at", 
        "id", "object_write_trace_id", "properties_with_history", "updated_at",
        "properties.amount", "properties.closedate", "properties.createdate", "properties.days_to_close", 
        "properties.dealname", "properties.dealstage", "properties.dealtype", "properties.description", 
        "properties.hs_lastmodifieddate", "properties.hs_object_id", "properties.pipeline"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Type conversions
    df["properties.amount"] = pd.to_numeric(df["properties.amount"], errors="coerce")
    df["properties.createdate"] = pd.to_datetime(df["properties.createdate"], errors="coerce")
    df["properties.closedate"] = pd.to_datetime(df["properties.closedate"], errors="coerce")
    df["properties.hs_lastmodifieddate"] = pd.to_datetime(df["properties.hs_lastmodifieddate"], errors="coerce")
    df["archived_at"] = pd.to_datetime(df["archived_at"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    return df[required_columns]

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Data Normalization")

    schema = {
        "archived": "string",
        "archived_at": "datetime64[ns]",
        "associations": "string",
        "created_at": "datetime64[ns]",
        "id": "string",
        "object_write_trace_id": "string",
        "properties_with_history": "string",
        "updated_at": "datetime64[ns]",
        "properties.amount": "float",
        "properties.closedate": "datetime64[ns]",
        "properties.createdate": "datetime64[ns]",
        "properties.days_to_close": "string",
        "properties.dealname": "string",
        "properties.dealstage": "string",
        "properties.dealtype": "string",
        "properties.description": "string",
        "properties.hs_lastmodifieddate": "datetime64[ns]",
        "properties.hs_object_id": "string",
        "properties.pipeline": "string"
    }

    for col, dtype in schema.items():
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype(dtype, errors="ignore")

    return df[list(schema.keys())]

# LOAD TO STAGING TABLE

def load_to_staging(df: pd.DataFrame, engine):
    logging.info("Loading data into staging table")

    # Type mapping
    dtype_map = {
        "created_at": DateTime(),
        "updated_at": DateTime(),
        "properties.closedate": DateTime(),
        "properties.createdate": DateTime(),
        "properties.hs_lastmodifieddate": DateTime(),
    }
    df.to_sql(
        name=STAGING_TABLE,
        schema="dbo",
        con=engine,
        if_exists="append",
        index=False,
        dtype=dtype_map,
        chunksize=1000
    )

    df_sql = pd.read_sql_table(STAGING_TABLE, con=engine, schema="dbo")
    print(df_sql)
   
    logging.info("Staging load completed")

# MERGE INTO FINAL TABLE

def merge_to_final(engine):
    logging.info("Merging staging into final table")

      
    merge_sql = f"""
        MERGE dbo.{FINAL_TABLE} WITH (HOLDLOCK) AS tgt
        USING (
            SELECT * FROM (
                    SELECT *,ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC
               ) AS rn
        FROM dbo.{STAGING_TABLE}
        ) s
        WHERE rn = 1
        ) AS src
        ON tgt.id = TRY_CAST(src.id AS BIGINT)

        WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
            UPDATE SET
                tgt.archived = src.archived,
                tgt.archived_at = src.archived_at,
                tgt.associations = src.associations,
                tgt.properties_with_history = src.properties_with_history,
                tgt.updated_at = src.updated_at,
                tgt.[properties.amount] = src.[properties.amount],
                tgt.[properties.closedate] = src.[properties.closedate],
                tgt.[properties.days_to_close] = src.[properties.days_to_close],
                tgt.[properties.dealname] = src.[properties.dealname],
                tgt.[properties.dealstage] = src.[properties.dealstage],
                tgt.[properties.dealtype] = src.[properties.dealtype],
                tgt.[properties.description] = src.[properties.description],
                tgt.[properties.hs_lastmodifieddate] = src.[properties.hs_lastmodifieddate],
                tgt.[properties.pipeline] = src.[properties.pipeline]

        WHEN NOT MATCHED
        AND TRY_CAST(src.id AS BIGINT) IS NOT NULL THEN
            INSERT (archived, archived_at, associations, created_at, 
            id, object_write_trace_id, properties_with_history, updated_at,
            [properties.amount], [properties.closedate], [properties.createdate], [properties.days_to_close], 
            [properties.dealname], [properties.dealstage], [properties.dealtype], [properties.description], 
            [properties.hs_lastmodifieddate], [properties.hs_object_id], [properties.pipeline])
            VALUES (src.archived, src.archived_at, src.associations, src.created_at,
            TRY_CAST(src.id AS BIGINT), src.object_write_trace_id, 
            src.properties_with_history, src.updated_at,
            src.[properties.amount], src.[properties.closedate], 
            src.[properties.createdate], src.[properties.days_to_close], src.[properties.dealname], 
            src.[properties.dealstage], src.[properties.dealtype], src.[properties.description], 
            src.[properties.hs_lastmodifieddate], src.[properties.hs_object_id], src.[properties.pipeline]
            );
     """


    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    logging.info("Merge completed")

# MAIN

def main():
    logging.info("HubSpot Records -> SQL Server ETL started")

    engine = get_engine()

    df = load_hubspot_json()
    df = clean_dataframe(df)
    df = normalize_dataframe(df)
    df = (df.sort_values("updated_at").drop_duplicates("id", keep="last"))

    load_to_staging(df, engine)
    merge_to_final(engine)

    logging.info("ETL completed successfully")

if __name__ == "__main__":
    main()