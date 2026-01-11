import logging
from sqlalchemy import create_engine, text, DateTime
from db import get_engine
from config import STAGING_TABLE, FINAL_TABLE
import pandas as pd

# LOAD TO STAGING TABLE

STAGING_TABLE = STAGING_TABLE
FINAL_TABLE = FINAL_TABLE

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
