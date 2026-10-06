-- =============================================================
-- rpt.usp_rpt_daily_production
-- rpt.daily_production: units + cases per line/day/sku.
-- cases = good_units / pack_size as INTEGER division (each per case).
-- yield_pct NULL when total = 0.
-- History: 2015-01 initial; 2019-07 cases on good units not total
-- (finance request); 2020-11 NULLIF guard on zero-total feeds.
-- =============================================================
CREATE OR ALTER PROCEDURE rpt.usp_rpt_daily_production
    @PipelineName VARCHAR(80) = 'PL_Daily_Production'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId INT;
    EXEC etl.usp_log_run @RunId OUTPUT, @PipelineName, 'rpt.usp_rpt_daily_production';

    BEGIN TRY
        TRUNCATE TABLE rpt.daily_production;

        INSERT INTO rpt.daily_production
        SELECT
            s.plant_id,
            s.line_id,
            s.production_day,
            s.sku_id,
            SUM(s.total_units) AS total_units,
            SUM(s.good_units)  AS good_units,
            SUM(s.good_units) / k.pack_size AS cases,
            ROUND(CAST(SUM(s.good_units) AS DECIMAL(18,4))
                  / NULLIF(SUM(s.total_units), 0) * 100, 2) AS yield_pct
        FROM stg.production_local s
        JOIN dim.sku k ON k.sku_id = s.sku_id
        GROUP BY s.plant_id, s.line_id, s.production_day, s.sku_id, k.pack_size;

        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_daily_production',
             @@ROWCOUNT, 'OK';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_daily_production',
             NULL, 'FAILED', ERROR_MESSAGE();
        THROW;
    END CATCH
    RETURN 0;
END
GO
