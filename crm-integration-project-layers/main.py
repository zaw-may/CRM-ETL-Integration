import logging
from db import get_engine
from hubspot_client import load_deals_json, load_hubspot_deal_pipelines, clean_dataframe, normalize_dataframe
from mssql_processing import load_to_staging, merge_to_final

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
