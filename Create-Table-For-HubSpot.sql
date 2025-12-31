USE [mydb]
GO

--- It is fine not to create the staging table. ---
CREATE TABLE [dbo].[stg_hubspot_deals](
	archived NVARCHAR(MAX),
	archived_at DATETIME,
	associations NVARCHAR(MAX),
	created_at DATETIME,
	id BIGINT NOT NULL,
	object_write_trace_id VARCHAR(MAX),
	properties_with_history VARCHAR(MAX),
	updated_at DATETIME,
	amount FLOAT,
	closedate DATETIME,
	createdate DATETIME,
	days_to_close VARCHAR(MAX),
	dealname VARCHAR(MAX),
	dealstage VARCHAR(MAX),
	dealtype VARCHAR(MAX),
	description VARCHAR(MAX),
	hs_lastmodifieddate DATETIME,
	hs_object_id VARCHAR(MAX),
	pipeline VARCHAR(MAX)
);

--- It is required to create the final table. ---
CREATE TABLE [dbo].[final_hubspot_deals] (
    archived VARCHAR(10),
    archived_at DATETIME,
    associations VARCHAR(20),
    created_at DATETIME,
    id BIGINT NOT NULL,
    object_write_trace_id VARCHAR(10),
    properties_with_history VARCHAR(20),
    updated_at DATETIME,
    [amount] FLOAT,
    [capacity_in_kwp] FLOAT,
    [closed_lost_reason] VARCHAR(255),
    [date_entered_stage_advanced_development] DATETIME,
    [date_entered_stage_closed_lost] DATETIME,
    [date_entered_stage_early_development] DATETIME,
    [date_entered_stage_potential_prospect] DATETIME,
    [date_entered_stage_project_approved] DATETIME,
    [date_exited_advanced_development] DATETIME,
    [date_exited_early_development] DATETIME,
    [date_exited_potential_prospect] DATETIME,
    [date_exited_project_approved] DATETIME,
    [dealname] VARCHAR(255),
    [dealstage] VARCHAR(255),
    [dealtype] VARCHAR(255),
    [final_capacity_in_kw] FLOAT,
    [hs_closed_amount] FLOAT,
    [hs_deal_stage_probability] FLOAT,
    [hs_forecast_amount] FLOAT,
    [hs_num_associated_deal_registrations] INT,
    [hs_num_associated_deal_splits] INT,
    [pipeline] VARCHAR(255),
    [ppa_capacity] FLOAT,
    [project_code] VARCHAR(255),
    [project_country] VARCHAR(50),
    [type_of_project_surface_type] VARCHAR(255),
    [hs_lastmodifieddate] DATETIME,
    [business_unit] VARCHAR(255),
    [capacity_in_mwp] FLOAT,
    [days_to_close] INT,
    [hs_is_closed] VARCHAR(255),
    [project_province] VARCHAR(255),
    [hs_projected_amount] FLOAT,
    [pipeline_name] VARCHAR(255), 
    [stage_name] VARCHAR(255),
    [stage_order] INT
);

ALTER TABLE [dbo].[final_hubspot_deals]
ADD CONSTRAINT uq_hubspot_deals_id UNIQUE (id);
