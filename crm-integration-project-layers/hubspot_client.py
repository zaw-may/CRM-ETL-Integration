import logging
from hubspot import HubSpot
from hubspot.crm.deals import ApiException
from hubspot.crm.pipelines import ApiException
from config import ACCESS_TOKEN
import pandas as pd

try:
    client = HubSpot(access_token=ACCESS_TOKEN)
except Exception as e:
    print(f"Error initializing HubSpot client: {e}")
    exit()

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
