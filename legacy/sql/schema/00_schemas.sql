-- Schemas for the MES reporting estate
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mes') EXEC('CREATE SCHEMA mes');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim') EXEC('CREATE SCHEMA dim');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg') EXEC('CREATE SCHEMA stg');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt') EXEC('CREATE SCHEMA rpt');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl') EXEC('CREATE SCHEMA etl');
GO
