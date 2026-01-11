"""
Steps:
0. Configure & connect DB
1. Load HubSpot records
2. Normalize & clean data
3. Load into staging table
4. MERGE into final table
"""

import pandas as pd
from sqlalchemy import create_engine, text, DateTime, Float, String, Integer
import logging
from hubspot import HubSpot
from hubspot.crm.deals import ApiException
from hubspot.crm.pipelines import ApiException
import urllib
from sqlalchemy.pool import StaticPool
import warnings
import os
from pathlib import Path
from dotenv import load_dotenv


# WARNING [Optional]

warnings.filterwarnings("ignore")

# CONFIGURATION

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
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
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

def load_deals_json() -> pd.DataFrame:
    logging.info("Loading HubSpot Deals JSON")
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
                    properties=["amount", "capacity_in_kwp", "closed_lost_reason__dropdown_", "date_entered_stage___advanced_development", "date_entered_stage___closed_lost", "date_entered_closing_advanced",
                            "date_entered_stage___early_development", "date_entered_stage___potential_prospect", "date_entered_stage___project_approved", "date_entered_stage___mid_development",
                            "date_entered_grid_checking_dpt", "date_entered_stage___operating", "date_entered_stage___ppa_1st_mark_up", "date_entered_preliminary_assessment", "date_entered_stage___ready_to_build", 
                            "date_entered_stage___testing", "date_entered_stage___under_construction", "hs_v2_date_entered_current_stage",
                            "date_exited_advanced_development", "date_exited_early_development", "date_exited_potential_prospect", "date_exited_project_approved", "date_exited_mid_development", 
                            "date_exited_grid_checking_dpt", "date_exited_operating", "date_exited_ppa_1st_mark_up", "date_exited_preliminary_assessment", "date_exited_ready_to_build", "date_exited_testing",
                            "date_exited_under_construction",
                            "dealname", "dealstage", "dealtype", "final_capacity_in_kw", "hs_closed_amount", "hs_deal_stage_probability", "hs_forecast_amount", 
                            "hs_num_associated_deal_registrations", "hs_num_associated_deal_splits", "pipeline", "ppa_capacity", "project_code", "project_country",
                            "type_of_project_surface_type__", "hs_lastmodifieddate", 
                            "business_unit", "capacity_in_mwp", "days_to_close", "hs_is_closed", "project_province", "hs_projected_amount"
                    ]
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
    logging.info(f"Loaded {len(df)} deals records")

    return flatten_df

def load_hubspot_deal_pipelines() -> pd.DataFrame:
    """
    Load HubSpot Deal Pipelines and Stages.
    """

    logging.info("Loading HubSpot Deals Pipeline JSON")

    try:
        api_response = client.crm.pipelines.pipelines_api.get_all(
            object_type="deals"
        )
    except ApiException as e:
        print(f"HubSpot Pipeline API error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error while loading pipelines: {e}")
        return pd.DataFrame()

    rows = []

    if not api_response or not api_response.results:
        print("No pipelines returned from HubSpot")
        return pd.DataFrame()

    for pipeline in api_response.results:
        pipeline_id = pipeline.id
        pipeline_name = pipeline.label

        # Safety check
        if not pipeline.stages:
            logging.info(f"Pipeline {pipeline_id} has no stages")
            continue

        for stage in pipeline.stages:
            rows.append({
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "stage_id": stage.id,
                "stage_name": stage.label,
                "stage_order": stage.display_order
                # "stage_probability": stage.metadata.get("probability"),
                # "is_closed_won": stage.metadata.get("isClosedWon"),
                # "is_closed_lost": stage.metadata.get("isClosedLost")
            })

    df = pd.DataFrame(rows)
    logging.info(f"Loaded {len(df)} pipeline-stage records")

    return df

# CLEAN DATA

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning data")

    # Ensure required columns exist
    required_columns = [
        "archived", "archived_at", "associations", "created_at", 
        "id", "object_write_trace_id", "properties_with_history", "updated_at",
        "properties.amount", "properties.capacity_in_kwp", "properties.closed_lost_reason__dropdown_", "properties.date_entered_stage___advanced_development",
        "properties.date_entered_stage___closed_lost", "properties.date_entered_closing_advanced", 
        "properties.date_entered_stage___early_development", "properties.date_entered_stage___potential_prospect", 
        "properties.date_entered_stage___project_approved", "properties.date_entered_stage___mid_development",
        "properties.date_entered_grid_checking_dpt", "properties.date_entered_stage___operating", "properties.date_entered_stage___ppa_1st_mark_up", 
        "properties.date_entered_preliminary_assessment", "properties.date_entered_stage___ready_to_build", 
        "properties.date_entered_stage___testing", "properties.date_entered_stage___under_construction", "properties.hs_v2_date_entered_current_stage",
        "properties.date_exited_advanced_development", "properties.date_exited_early_development",
        "properties.date_exited_potential_prospect", "properties.date_exited_project_approved", "properties.date_exited_mid_development", 
        "properties.date_exited_grid_checking_dpt", "properties.date_exited_operating", "properties.date_exited_ppa_1st_mark_up", "properties.date_exited_preliminary_assessment", 
        "properties.date_exited_ready_to_build", "date_exited_testing", "properties.date_exited_under_construction",                     
        "properties.dealname", "properties.dealstage", "properties.dealtype",
        "properties.final_capacity_in_kw", "properties.hs_closed_amount", "properties.hs_deal_stage_probability", "properties.hs_forecast_amount", 
        "properties.hs_num_associated_deal_registrations", "properties.hs_num_associated_deal_splits", "properties.pipeline", "properties.ppa_capacity", 
        "properties.project_code", "properties.project_country", "properties.type_of_project_surface_type__", "properties.hs_lastmodifieddate",
        "properties.business_unit", "properties.capacity_in_mwp", "properties.days_to_close", "properties.hs_is_closed", 
        "properties.project_province", "properties.hs_projected_amount", "pipeline_name", "stage_name", "stage_order"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Type conversions
    df["properties.amount"] = pd.to_numeric(df["properties.amount"], errors="coerce")
    df["properties.capacity_in_kwp"] = pd.to_numeric(df["properties.capacity_in_kwp"], errors="coerce")
    df["properties.final_capacity_in_kw"] = pd.to_numeric(df["properties.final_capacity_in_kw"], errors="coerce")
    df["properties.hs_closed_amount"] = pd.to_numeric(df["properties.hs_closed_amount"], errors="coerce")
    df["properties.hs_deal_stage_probability"] = pd.to_numeric(df["properties.hs_deal_stage_probability"], errors="coerce")
    df["properties.hs_forecast_amount"] = pd.to_numeric(df["properties.hs_forecast_amount"], errors="coerce")
    df["properties.hs_num_associated_deal_registrations"] = pd.to_numeric(df["properties.hs_num_associated_deal_registrations"], errors="coerce")
    df["properties.hs_num_associated_deal_splits"] = pd.to_numeric(df["properties.hs_num_associated_deal_splits"], errors="coerce")
    df["properties.ppa_capacity"] = pd.to_numeric(df["properties.ppa_capacity"], errors="coerce")
    df["properties.capacity_in_mwp"] = pd.to_numeric(df["properties.capacity_in_mwp"], errors="coerce")
    df["properties.days_to_close"] = pd.to_numeric(df["properties.days_to_close"], errors="coerce")
    df["properties.hs_projected_amount"] = pd.to_numeric(df["properties.hs_projected_amount"], errors="coerce")
    df["stage_order"] = pd.to_numeric(df["stage_order"], errors="coerce")
    df["properties.date_entered_stage___advanced_development"] = pd.to_datetime(df["properties.date_entered_stage___advanced_development"], errors="coerce")
    df["properties.date_entered_stage___closed_lost"] = pd.to_datetime(df["properties.date_entered_stage___closed_lost"], errors="coerce")
    df["properties.date_entered_closing_advanced"] = pd.to_datetime(df["properties.date_entered_closing_advanced"], errors="coerce")  
    df["properties.date_entered_stage___early_development"] = pd.to_datetime(df["properties.date_entered_stage___early_development"], errors="coerce")
    df["properties.date_entered_stage___potential_prospect"] = pd.to_datetime(df["properties.date_entered_stage___potential_prospect"], errors="coerce")
    df["properties.date_entered_stage___project_approved"] = pd.to_datetime(df["properties.date_entered_stage___project_approved"], errors="coerce")
    df["properties.date_entered_stage___mid_development"] = pd.to_datetime(df["properties.date_entered_stage___mid_development"], errors="coerce")
    df["properties.date_entered_grid_checking_dpt"] = pd.to_datetime(df["properties.date_entered_grid_checking_dpt"], errors="coerce")
    df["properties.date_entered_stage___operating"] = pd.to_datetime(df["properties.date_entered_stage___operating"], errors="coerce")
    df["properties.date_entered_stage___ppa_1st_mark_up"] = pd.to_datetime(df["properties.date_entered_stage___ppa_1st_mark_up"], errors="coerce")
    df["properties.date_entered_preliminary_assessment"] = pd.to_datetime(df["properties.date_entered_preliminary_assessment"], errors="coerce")
    df["properties.date_entered_stage___ready_to_build"] = pd.to_datetime(df["properties.date_entered_stage___ready_to_build"], errors="coerce")
    df["properties.date_entered_stage___testing"] = pd.to_datetime(df["properties.date_entered_stage___testing"], errors="coerce")
    df["properties.date_entered_stage___under_construction"] = pd.to_datetime(df["properties.date_entered_stage___under_construction"], errors="coerce")
    df["properties.hs_v2_date_entered_current_stage"] = pd.to_datetime(df["properties.hs_v2_date_entered_current_stage"], errors="coerce")
    df["properties.date_exited_advanced_development"] = pd.to_datetime(df["properties.date_exited_advanced_development"], errors="coerce")
    df["properties.date_exited_early_development"] = pd.to_datetime(df["properties.date_exited_early_development"], errors="coerce")
    df["properties.date_exited_potential_prospect"] = pd.to_datetime(df["properties.date_exited_potential_prospect"], errors="coerce")
    df["properties.date_exited_project_approved"] = pd.to_datetime(df["properties.date_exited_project_approved"], errors="coerce")
    df["properties.date_exited_mid_development"] = pd.to_datetime(df["properties.date_exited_mid_development"], errors="coerce")
    df["properties.date_exited_grid_checking_dpt"] = pd.to_datetime(df["properties.date_exited_grid_checking_dpt"], errors="coerce")
    df["properties.date_exited_operating"] = pd.to_datetime(df["properties.date_exited_operating"], errors="coerce")
    df["properties.date_exited_ppa_1st_mark_up"] = pd.to_datetime(df["properties.date_exited_mid_development"], errors="coerce")
    df["properties.date_exited_preliminary_assessment"] = pd.to_datetime(df["properties.date_exited_preliminary_assessment"], errors="coerce")
    df["properties.date_exited_ready_to_build"] = pd.to_datetime(df["properties.date_exited_ready_to_build"], errors="coerce")
    df["properties.date_exited_testing"] = pd.to_datetime(df["properties.date_exited_testing"], errors="coerce")
    df["properties.date_exited_under_construction"] = pd.to_datetime(df["properties.date_exited_under_construction"], errors="coerce")
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
        "properties.capacity_in_kwp": "float",
        "properties.closed_lost_reason__dropdown_": "string",
        "properties.date_entered_stage___advanced_development": "datetime64[ns]",
        "properties.date_entered_stage___closed_lost": "datetime64[ns]",
        "properties.date_entered_closing_advanced": "datetime64[ns]",
        "properties.date_entered_stage___early_development": "datetime64[ns]",
        "properties.date_entered_stage___potential_prospect": "datetime64[ns]",
        "properties.date_entered_stage___project_approved": "datetime64[ns]",
        "properties.date_entered_stage___mid_development": "datetime64[ns]",
        "properties.date_entered_grid_checking_dpt": "datetime64[ns]",
        "properties.date_entered_stage___operating": "datetime64[ns]",
        "properties.date_entered_stage___ppa_1st_mark_up": "datetime64[ns]",
        "properties.date_entered_preliminary_assessment": "datetime64[ns]",
        "properties.date_entered_stage___ready_to_build": "datetime64[ns]",
        "properties.date_entered_stage___testing": "datetime64[ns]",
        "properties.date_entered_stage___under_construction": "datetime64[ns]",
        "properties.hs_v2_date_entered_current_stage": "datetime64[ns]",
        "properties.date_exited_advanced_development": "datetime64[ns]",
        "properties.date_exited_early_development": "datetime64[ns]",
        "properties.date_exited_potential_prospect": "datetime64[ns]",
        "properties.date_exited_project_approved": "datetime64[ns]",
        "properties.date_exited_mid_development": "datetime64[ns]",
        "properties.date_exited_grid_checking_dpt": "datetime64[ns]",
        "properties.date_exited_operating": "datetime64[ns]",
        "properties.date_exited_ppa_1st_mark_up": "datetime64[ns]",
        "properties.date_exited_preliminary_assessment": "datetime64[ns]",
        "properties.date_exited_ready_to_build": "datetime64[ns]",
        "properties.date_exited_testing": "datetime64[ns]",
        "properties.date_exited_under_construction": "datetime64[ns]",
        "properties.dealname": "string",
        "properties.dealstage": "string",
        "properties.dealtype": "string",
        "properties.final_capacity_in_kw": "float",
        "properties.hs_closed_amount": "float",
        "properties.hs_deal_stage_probability": "float",
        "properties.hs_forecast_amount": "float",
        "properties.hs_num_associated_deal_registrations": "int64",
        "properties.hs_num_associated_deal_splits": "int64",
        "properties.pipeline": "string",
        "properties.ppa_capacity": "float",
        "properties.project_code": "string",
        "properties.project_country": "string",
        "properties.type_of_project_surface_type__": "string",
        "properties.hs_lastmodifieddate": "datetime64[ns]",
        "properties.business_unit": "string",
        "properties.capacity_in_mwp": "float",
        "properties.days_to_close": "int64",
        "properties.hs_is_closed": "string", 
        "properties.project_province": "string",
        "properties.hs_projected_amount": "float",
        "pipeline_name": "string",
        "stage_name": "string",
        "stage_order": "int64"
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
        "archived_at": DateTime(),
        "created_at": DateTime(), 
        "updated_at": DateTime(),
        "properties.date_entered_stage___advanced_development": DateTime(),
        "properties.date_entered_stage___closed_lost": DateTime(),
        "properties.date_entered_closing_advanced": DateTime(),
        "properties.date_entered_stage___early_development": DateTime(),
        "properties.date_entered_stage___potential_prospect": DateTime(),
        "properties.date_entered_stage___project_approved": DateTime(),
        "properties.date_entered_stage___mid_development": DateTime(),
        "properties.date_entered_grid_checking_dpt": DateTime(),
        "properties.date_entered_stage___operating": DateTime(),
        "properties.date_entered_stage___ppa_1st_mark_up": DateTime(),
        "properties.date_entered_preliminary_assessment": DateTime(),
        "properties.date_entered_stage___ready_to_build": DateTime(),
        "properties.date_entered_stage___testing": DateTime(),
        "properties.date_entered_stage___under_construction": DateTime(),
        "properties.hs_v2_date_entered_current_stage": DateTime(),
        "properties.date_exited_advanced_development": DateTime(),
        "properties.date_exited_early_development": DateTime(),
        "properties.date_exited_potential_prospect": DateTime(),
        "properties.date_exited_project_approved": DateTime(),
        "properties.date_exited_mid_development": DateTime(),
        "properties.date_exited_grid_checking_dpt": DateTime(),
        "properties.date_exited_operating": DateTime(),
        "properties.date_exited_ppa_1st_mark_up": DateTime(),
        "properties.date_exited_preliminary_assessment": DateTime(),
        "properties.date_exited_ready_to_build": DateTime(),
        "properties.date_exited_testing": DateTime(),
        "properties.date_exited_under_construction": DateTime(),
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
            SELECT * FROM 
                (
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
                tgt.[amount] = src.[properties.amount],
                tgt.[capacity_in_kwp] = src.[properties.capacity_in_kwp],
                tgt.[closed_lost_reason] = src.[properties.closed_lost_reason__dropdown_],
                tgt.[date_entered_stage_advanced_development] = src.[properties.date_entered_stage___advanced_development],
                tgt.[date_entered_stage_closed_lost] = src.[properties.date_entered_stage___closed_lost],
                tgt.[date_entered_closing_advanced] = src.[properties.date_entered_closing_advanced],
                tgt.[date_entered_stage_early_development] = src.[properties.date_entered_stage___early_development],
                tgt.[date_entered_stage_potential_prospect] = src.[properties.date_entered_stage___potential_prospect],
                tgt.[date_entered_stage_project_approved] = src.[properties.date_entered_stage___project_approved],
                tgt.[date_entered_stage_mid_development] = src.[properties.date_entered_stage___mid_development],
                tgt.[date_entered_grid_checking_dpt] = src.[properties.date_entered_grid_checking_dpt],
                tgt.[date_entered_stage_operating] = src.[properties.date_entered_stage___operating],
                tgt.[date_entered_stage_ppa_1st_mark_up] = src.[properties.date_entered_stage___ppa_1st_mark_up],
                tgt.[date_entered_preliminary_assessment] = src.[properties.date_entered_preliminary_assessment],
                tgt.[date_entered_stage_ready_to_build] = src.[properties.date_entered_stage___ready_to_build],
                tgt.[date_entered_stage_testing] = src.[properties.date_entered_stage___testing],
                tgt.[date_entered_stage_under_construction] = src.[properties.date_entered_stage___under_construction],
                tgt.[hs_v2_date_entered_current_stage] = src.[properties.hs_v2_date_entered_current_stage],
                tgt.[date_exited_advanced_development] = src.[properties.date_exited_advanced_development],
                tgt.[date_exited_early_development] = src.[properties.date_exited_early_development],
                tgt.[date_exited_potential_prospect] = src.[properties.date_exited_potential_prospect],
                tgt.[date_exited_project_approved] = src.[properties.date_exited_project_approved],
                tgt.[date_exited_mid_development] = src.[properties.date_exited_mid_development],
                tgt.[date_exited_grid_checking_dpt] = src.[properties.date_exited_grid_checking_dpt],
                tgt.[date_exited_operating] = src.[properties.date_exited_operating],
                tgt.[date_exited_ppa_1st_mark_up] = src.[properties.date_exited_ppa_1st_mark_up],
                tgt.[date_exited_preliminary_assessment] = src.[properties.date_exited_preliminary_assessment],
                tgt.[date_exited_ready_to_build] = src.[properties.date_exited_ready_to_build],
                tgt.[date_exited_testing] = src.[properties.date_exited_testing],
                tgt.[date_exited_under_construction] = src.[properties.date_exited_under_construction],
                tgt.[dealname] = src.[properties.dealname],
                tgt.[dealstage] = src.[properties.dealstage],
                tgt.[dealtype] = src.[properties.dealtype],
                tgt.[final_capacity_in_kw] = src.[properties.final_capacity_in_kw],
                tgt.[hs_closed_amount] = src.[properties.hs_closed_amount],
                tgt.[hs_deal_stage_probability] = src.[properties.hs_deal_stage_probability],
                tgt.[hs_forecast_amount] = src.[properties.hs_forecast_amount],
                tgt.[hs_num_associated_deal_registrations] = src.[properties.hs_num_associated_deal_registrations],
                tgt.[hs_num_associated_deal_splits] = src.[properties.hs_num_associated_deal_splits],
                tgt.[pipeline] = src.[properties.pipeline],
                tgt.[ppa_capacity] = src.[properties.ppa_capacity],
                tgt.[project_code] = src.[properties.project_code],
                tgt.[project_country] = src.[properties.project_country],
                tgt.[type_of_project_surface_type] = src.[properties.type_of_project_surface_type__],
                tgt.[hs_lastmodifieddate] = src.[properties.hs_lastmodifieddate],
                tgt.[business_unit] = src.[properties.business_unit],
                tgt.[capacity_in_mwp] = src.[properties.capacity_in_mwp],
                tgt.[days_to_close] = src.[properties.days_to_close],
                tgt.[hs_is_closed] = src.[properties.hs_is_closed],
                tgt.[project_province] = src.[properties.project_province],
                tgt.[hs_projected_amount] = src.[properties.hs_projected_amount],
                tgt.[pipeline_name] = src.[pipeline_name], 
                tgt.[stage_name] = src.[stage_name],
                tgt.[stage_order] = src.[stage_order]

        WHEN NOT MATCHED
        AND TRY_CAST(src.id AS BIGINT) IS NOT NULL THEN
            INSERT (
                archived,
                archived_at,
                associations,
                created_at,
                id,
                object_write_trace_id,
                properties_with_history,
                updated_at,
                [amount],
                [capacity_in_kwp],
                [closed_lost_reason],
                [date_entered_stage_advanced_development],
                [date_entered_stage_closed_lost],
                [date_entered_closing_advanced],
                [date_entered_stage_early_development],
                [date_entered_stage_potential_prospect],
                [date_entered_stage_project_approved],
                [date_entered_stage_mid_development],
                [date_entered_grid_checking_dpt],
                [date_entered_stage_operating],
                [date_entered_stage_ppa_1st_mark_up],
                [date_entered_preliminary_assessment],
                [date_entered_stage_ready_to_build],
                [date_entered_stage_testing],
                [date_entered_stage_under_construction],
                [hs_v2_date_entered_current_stage],
                [date_exited_advanced_development],
                [date_exited_early_development],
                [date_exited_potential_prospect],
                [date_exited_project_approved],
                [date_exited_mid_development],
                [date_exited_grid_checking_dpt],
                [date_exited_operating],
                [date_exited_ppa_1st_mark_up],
                [date_exited_preliminary_assessment],
                [date_exited_ready_to_build],
                [date_exited_testing],
                [date_exited_under_construction],
                [dealname],
                [dealstage],
                [dealtype],
                [final_capacity_in_kw],
                [hs_closed_amount],
                [hs_deal_stage_probability],
                [hs_forecast_amount],
                [hs_num_associated_deal_registrations],
                [hs_num_associated_deal_splits],
                [pipeline],
                [ppa_capacity],
                [project_code],
                [project_country],
                [type_of_project_surface_type],
                [hs_lastmodifieddate],
                [business_unit],
                [capacity_in_mwp],
                [days_to_close],
                [hs_is_closed],
                [project_province],
                [hs_projected_amount],
                [pipeline_name], 
                [stage_name],
                [stage_order]
            )
            VALUES (
                src.archived,
                src.archived_at,
                src.associations,
                src.created_at,
                TRY_CAST(src.id AS BIGINT),
                src.object_write_trace_id,
                src.properties_with_history,
                src.updated_at,
                src.[properties.amount],
                src.[properties.capacity_in_kwp],
                src.[properties.closed_lost_reason__dropdown_],
                src.[properties.date_entered_stage___advanced_development],
                src.[properties.date_entered_stage___closed_lost],
                src.[properties.date_entered_closing_advanced],                
                src.[properties.date_entered_stage___early_development],
                src.[properties.date_entered_stage___potential_prospect],
                src.[properties.date_entered_stage___project_approved],
                src.[properties.date_entered_stage___mid_development],
                src.[properties.date_entered_grid_checking_dpt],
                src.[properties.date_entered_stage___operating],
                src.[properties.date_entered_stage___ppa_1st_mark_up],
                src.[properties.date_entered_preliminary_assessment],
                src.[properties.date_entered_stage___ready_to_build],
                src.[properties.date_entered_stage___testing],
                src.[properties.date_entered_stage___under_construction],
                src.[properties.hs_v2_date_entered_current_stage],
                src.[properties.date_exited_advanced_development],
                src.[properties.date_exited_early_development],
                src.[properties.date_exited_potential_prospect],
                src.[properties.date_exited_project_approved],
                src.[properties.date_exited_mid_development],
                src.[properties.date_exited_grid_checking_dpt],
                src.[properties.date_exited_operating],
                src.[properties.date_exited_ppa_1st_mark_up],
                src.[properties.date_exited_preliminary_assessment],
                src.[properties.date_exited_ready_to_build],
                src.[properties.date_exited_testing],
                src.[properties.date_exited_under_construction],
                src.[properties.dealname],
                src.[properties.dealstage],
                src.[properties.dealtype],
                src.[properties.final_capacity_in_kw],
                src.[properties.hs_closed_amount],
                src.[properties.hs_deal_stage_probability],
                src.[properties.hs_forecast_amount],
                src.[properties.hs_num_associated_deal_registrations],
                src.[properties.hs_num_associated_deal_splits],
                src.[properties.pipeline],
                src.[properties.ppa_capacity],
                src.[properties.project_code],
                src.[properties.project_country],
                src.[properties.type_of_project_surface_type__],
                src.[properties.hs_lastmodifieddate],
                src.[properties.business_unit],
                src.[properties.capacity_in_mwp],
                src.[properties.days_to_close],
                src.[properties.hs_is_closed],
                src.[properties.project_province],
                src.[properties.hs_projected_amount],
                src.[pipeline_name], 
                src.[stage_name],
                src.[stage_order]
            );
     """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    logging.info("Merge completed")

# MAIN

def main():
    logging.info("HubSpot Records -> SQL Server ETL started")

    engine = get_engine()

    df = load_deals_json()
    df_pipelines = load_hubspot_deal_pipelines()

    df_final = ( df.merge (
            df_pipelines,
            left_on=["properties.pipeline", "properties.dealstage"],
            right_on=["pipeline_id", "stage_id"],
            how="left"
            )
            .drop(columns=["pipeline_id", "stage_id"])
        )

    df_final = clean_dataframe(df_final)
    df_final = normalize_dataframe(df_final)
    df_final = (df_final.sort_values("updated_at").drop_duplicates("id", keep="last"))

    load_to_staging(df_final, engine)
    merge_to_final(engine)

    logging.info("ETL completed successfully")

if __name__ == "__main__":
    main()