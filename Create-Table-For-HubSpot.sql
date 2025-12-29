USE [mydb]
GO

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

CREATE TABLE [dbo].[hubspot_deals](
	archived NVARCHAR(MAX),
	archived_at DATETIME,
	associations NVARCHAR(MAX),
	created_at DATETIME,
	id BIGINT NOT NULL,
	object_write_trace_id VARCHAR(MAX),
	properties_with_history VARCHAR(MAX),
	updated_at DATETIME,
	[properties.amount] FLOAT,
	[properties.closedate] DATETIME,
	[properties.createdate] DATETIME,
	[properties.days_to_close] VARCHAR(MAX),
	[properties.dealname] VARCHAR(MAX),
	[properties.dealstage] VARCHAR(MAX),
	[properties.dealtype] VARCHAR(MAX),
	[properties.description] VARCHAR(MAX),
	[properties.hs_lastmodifieddate] DATETIME,
	[properties.hs_object_id] VARCHAR(MAX),
	[properties.pipeline] VARCHAR(MAX)
);

ALTER TABLE dbo.hubspot_deals
ADD CONSTRAINT uq_hubspot_deals_id UNIQUE (id);
