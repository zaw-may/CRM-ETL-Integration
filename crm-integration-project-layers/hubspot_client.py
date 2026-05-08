# Process03

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
                    properties=[ 
                          "capacity_in_kwp" # Initial: Required data fields from HubSpot 
                        , "closed_lost_reason__dropdown_"
                        , "closedate" 
                        , "date_entered_stage___advanced_development"
                        , "date_entered_stage___closed_lost"
                        , "date_entered_closing_advanced"
                        , "date_entered_stage___early_development"
                        , "date_entered_stage___potential_prospect"
                        , "date_entered_stage___project_approved"
                        , "date_entered_stage___mid_development"
                        , "date_entered_grid_checking_dpt"
                        , "date_entered_stage___operating"
                        , "date_entered_stage___ppa_1st_mark_up"
                        , "date_entered_preliminary_assessment"
                        , "date_entered_stage___ready_to_build"
                        , "date_entered_stage___testing"
                        , "date_entered_stage___under_construction"
                        , "date_entered_bid_submitted"
                        , "date_entered_non_binding_offer_early"
                        , "date_entered_project_awarded"
                        , "date_entered_binding_offer_spa_mid"
                        , "date_entered_ppa_signed"
                        , "date_entered_invoiced"
                        , "expected_ppa_signature"
                        , "hs_v2_date_entered_current_stage"
                        , "cp_issued"
                        , "date_exited_advanced_development"
                        , "date_exited_closing_advanced"
                        , "date_exited_early_development"
                        , "date_exited_potential_prospect"
                        , "date_exited_project_approved"
                        , "date_exited_mid_development"
                        , "date_exited_grid_checking_dpt"
                        , "date_exited_operating"
                        , "date_exited_ppa_1st_mark_up"
                        , "date_exited_preliminary_assessment"
                        , "date_exited_ready_to_build"
                        , "date_exited_testing"
                        , "date_exited_under_construction"
                        , "date_exited_bid_submitted"
                        , "date_exited_non_binding_offer_early"
                        , "date_exited_project_awarded"
                        , "date_exited_binding_offer_spa_mid"
                        , "date_exited_ppa_signed"
                        , "date_exited_invoiced"
                        , "planned_date_of_invoicing"
                        , "dealname"
                        , "dealstage" # Join Key
                        , "final_capacity_in_kw"
                        , "hs_closed_amount"
                        , "hs_deal_stage_probability"
                        , "pipeline" # Join Key
                        , "ppa_capacity"
                        , "probability_of_closing"
                        , "project_code"
                        , "project_country"
                        , "type_of_project_surface_type__"
                        , "type_of_commitment"
                        , "growth_type"
                        , "hs_lastmodifieddate"
                        , "business_unit"
                        , "business_line"
                        , "capacity_in_mwp"
                        , "days_to_close"
                        , "hs_is_closed"
                        , "project_province"
                        , "associated_company"
                        , "included_in_mis"
                        , "gps"
                        , "technology"
                        , "ma_greenfield__brownfield"
                        , "ce_entity"
                        , "hs_v2_date_entered_172697438" # Start: New DealStage Date
                        , "hs_v2_date_entered_172697439"
                        , "hs_v2_date_entered_172697440"
                        , "hs_v2_date_entered_172697441"
                        , "hs_v2_date_entered_172697443"
                        , "hs_v2_date_entered_181472374"
                        , "hs_v2_date_entered_183929950"
                        , "hs_v2_date_entered_3271943924"
                        , "hs_v2_date_entered_3275128565"
                        , "hs_v2_date_entered_947733654"
                        , "hs_v2_date_entered_947733655"
                        , "hs_v2_date_entered_947733656"
                        , "hs_v2_date_entered_947761553"
                        , "hs_v2_date_entered_953835685"
                        , "hs_v2_date_entered_953835686"
                        , "hs_v2_date_entered_953835687"
                        , "hs_v2_date_entered_953835688"
                        , "hs_v2_date_entered_953835689"
                        , "hs_v2_date_entered_953835691"
                        , "hs_v2_date_entered_953840641"
                        , "hs_v2_date_entered_962990195"
                        , "hs_v2_date_entered_987141709"
                        , "hs_v2_date_entered_987141711"
                        , "hs_v2_date_entered_987141713"
                        , "hs_v2_date_entered_987141714"
                        , "hs_v2_date_entered_987141715"
                        , "hs_v2_date_entered_989326996"
                        , "hs_v2_date_entered_989326997"
                        , "hs_v2_date_entered_989326998"
                        , "hs_v2_date_entered_989326999"
                        , "hs_v2_date_entered_997789966"
                        , "hs_v2_date_exited_172697438"
                        , "hs_v2_date_exited_172697439"
                        , "hs_v2_date_exited_172697440"
                        , "hs_v2_date_exited_172697441"
                        , "hs_v2_date_exited_172697443"
                        , "hs_v2_date_exited_181472374"
                        , "hs_v2_date_exited_183929950"
                        , "hs_v2_date_exited_3271943924"
                        , "hs_v2_date_exited_3275128565"
                        , "hs_v2_date_exited_947733654"
                        , "hs_v2_date_exited_947733655"
                        , "hs_v2_date_exited_947733656"
                        , "hs_v2_date_exited_947761553"
                        , "hs_v2_date_exited_953835685"
                        , "hs_v2_date_exited_953835686"
                        , "hs_v2_date_exited_953835687"
                        , "hs_v2_date_exited_953835688"
                        , "hs_v2_date_exited_953835689"
                        , "hs_v2_date_exited_953835691"
                        , "hs_v2_date_exited_953840641"
                        , "hs_v2_date_exited_962990195"
                        , "hs_v2_date_exited_987141709"
                        , "hs_v2_date_exited_987141711"
                        , "hs_v2_date_exited_987141713"
                        , "hs_v2_date_exited_987141714"
                        , "hs_v2_date_exited_987141715"
                        , "hs_v2_date_exited_989326996"
                        , "hs_v2_date_exited_989326997"
                        , "hs_v2_date_exited_989326998"
                        , "hs_v2_date_exited_989326999"
                        , "hs_v2_date_exited_997789966" # End: New DealStage Date
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
    required_other_columns = [
        "archived", 
        "archived_at", 
        "associations", 
        "created_at", 
        "updated_at",
        "properties.hs_lastmodifieddate",
        "id", 
        "object_write_trace_id", 
        "properties_with_history", 
        "properties.capacity_in_kwp", 
        "properties.closed_lost_reason__dropdown_", 
        "properties.closedate",
        "properties.expected_ppa_signature", 
        "properties.cp_issued",
        "properties.date_of_invoicing", 
        "properties.planned_date_of_invoicing",
        "properties.dealname", 
        "properties.dealstage", 
        "properties.final_capacity_in_kw", 
        "properties.hs_closed_amount", 
        "properties.hs_deal_stage_probability",
        "properties.pipeline", 
        "properties.ppa_capacity", 
        "properties.probability_of_closing",
        "properties.project_code", 
        "properties.project_country", 
        "properties.type_of_project_surface_type__", 
        "properties.type_of_commitment", 
        "properties.growth_type",
        "properties.business_unit", 
        "properties.business_line",
        "properties.capacity_in_mwp", 
        "properties.days_to_close", 
        "properties.hs_is_closed", 
        "properties.project_province", 
        "properties.associated_company", 
        "properties.included_in_mis",
        "properties.gps",
        "properties.technology", 
        "properties.ma_greenfield__brownfield",
        "properties.ce_entity",
        "pipeline_name", 
        "stage_name", 
        "stage_order",
    ]

    # Ensure required date columns exist
    hs_v2_date_entered_cols = [c for c in df.columns if c.startswith("properties.hs_v2_date_entered_")]
    hs_v2_date_exited_cols = [c for c in df.columns if c.startswith("properties.hs_v2_date_exited_")]
    normal_date_entered_cols = [c for c in df.columns if c.startswith("properties.date_entered_")]
    normal_date_exited_cols = [c for c in df.columns if c.startswith("properties.date_exited_")]
    all_date_cols = hs_v2_date_entered_cols + hs_v2_date_exited_cols + normal_date_entered_cols + normal_date_exited_cols
    required_all_columns = required_other_columns + all_date_cols

    for col in required_all_columns:
        if col not in df.columns:
            df[col] = None

    # Type conversions for special date columns
    for col in all_date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Type conversions for the rest columns
    df["properties.ppa_capacity"] = pd.to_numeric(df["properties.ppa_capacity"], errors="coerce")
    df["properties.final_capacity_in_kw"] = pd.to_numeric(df["properties.final_capacity_in_kw"], errors="coerce")
    df["properties.capacity_in_kwp"] = pd.to_numeric(df["properties.capacity_in_kwp"], errors="coerce")
    df["properties.capacity_in_mwp"] = pd.to_numeric(df["properties.capacity_in_mwp"], errors="coerce")
    df["properties.hs_closed_amount"] = pd.to_numeric(df["properties.hs_closed_amount"], errors="coerce")
    df["properties.probability_of_closing"] = pd.to_numeric(df["properties.probability_of_closing"], errors="coerce")
    df["properties.hs_deal_stage_probability"] = pd.to_numeric(df["properties.hs_deal_stage_probability"], errors="coerce")
    df["properties.days_to_close"] = pd.to_numeric(df["properties.days_to_close"], errors="coerce")
    #df["stage_order"] = pd.to_numeric(df["stage_order"], errors="coerce")
    df["properties.expected_ppa_signature"] = pd.to_datetime(df["properties.expected_ppa_signature"], errors="coerce")
    df["properties.cp_issued"] = pd.to_datetime(df["properties.cp_issued"], errors="coerce")  
    df["properties.planned_date_of_invoicing"] = pd.to_datetime(df["properties.planned_date_of_invoicing"], errors="coerce")
    df["properties.closedate"] = pd.to_datetime(df["properties.closedate"], errors="coerce")  
    df["properties.hs_lastmodifieddate"] = pd.to_datetime(df["properties.hs_lastmodifieddate"], errors="coerce")
    df["archived_at"] = pd.to_datetime(df["archived_at"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    return df[required_all_columns]

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    logging.info("Data Normalization with schema")

    schema = {
        "id": "string", 
        "object_write_trace_id": "string",
        "properties_with_history": "string",
        "associations": "string",
        "properties.associated_company": "string",
        "properties.project_code": "string",
        "properties.dealname": "string",
        "properties.pipeline": "string",
        "pipeline_name": "string",
        "properties.dealstage": "string",   
        "stage_name": "string",
        "stage_order": "string",
        "properties.ce_entity": "string",
        "properties.business_unit": "string",
        "properties.business_line": "string",
        "properties.project_country": "string",
        "properties.project_province": "string",    
        "properties.ppa_capacity": "float",
        "properties.final_capacity_in_kw": "float",
        "properties.capacity_in_kwp": "float",
        "properties.capacity_in_mwp": "float",
        "properties.hs_closed_amount": "float",
        "properties.probability_of_closing": "float",
        "properties.hs_deal_stage_probability": "float",
        "properties.technology": "string",
        "properties.ma_greenfield__brownfield": "string",
        "properties.growth_type": "string",
        "properties.type_of_project_surface_type__": "string",
        "properties.type_of_commitment": "string",
        "properties.closed_lost_reason__dropdown_": "string",
        "properties.days_to_close": "int64",
        "properties.hs_is_closed": "string", 
        "properties.included_in_mis": "string",
        "properties.gps": "string", 
        "archived": "string",
        "archived_at": "datetime64[ns]",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
        "properties.hs_lastmodifieddate": "datetime64[ns]",
        "properties.closedate": "datetime64[ns]",
        "properties.expected_ppa_signature": "datetime64[ns]",
        "properties.cp_issued": "datetime64[ns]",
        "properties.planned_date_of_invoicing": "datetime64[ns]",
}

    hs_v2_date_entered_cols = [c for c in df.columns if c.startswith("properties.hs_v2_date_entered_")]
    hs_v2_date_exited_cols = [c for c in df.columns if c.startswith("properties.hs_v2_date_exited_")]
    normal_date_entered_cols = [c for c in df.columns if c.startswith("properties.date_entered_")]
    normal_date_exited_cols = [c for c in df.columns if c.startswith("properties.date_exited_")]

    all_date_cols = (
        hs_v2_date_entered_cols +
        hs_v2_date_exited_cols +
        normal_date_entered_cols +
        normal_date_exited_cols
    )

    # Add date columns to schema
    schema.update({col: "datetime64[ns]" for col in all_date_cols})

    # Validate schema items
    # print("Schema Items: ", schema.items())

    # ensure all columns exist
    for col, dtype in schema.items():
        if col not in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.NaT
            else:
                df[col] = pd.NA

    for col, dtype in schema.items():
        if dtype == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = df[col].astype(dtype)

    
    return df[list(schema.keys())]