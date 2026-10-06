-- =============================================================
-- rpt.usp_rpt_scrap_yield_weekly
-- Weekly scrap vs good units per line, ISO week via
-- DATEPART(ISO_WEEK). scrap_pct NULL when denominator 0.
-- History: 2016-02 initial; 2020-05 switched fiscal wk -> ISO_WEEK.
-- =============================================================
CREATE OR ALTER PROCEDURE rpt.usp_rpt_scrap_yield_weekly
    @PipelineName VARCHAR(80) = 'PL_Scrap_Yield'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId INT;
    EXEC etl.usp_log_run @RunId OUTPUT, @PipelineName, 'rpt.usp_rpt_scrap_yield_weekly';

    BEGIN TRY
        TRUNCATE TABLE rpt.scrap_yield_weekly;

        ;WITH scrap AS (
            SELECT l.plant_id, s.line_id,
                   YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, s.scrap_ts_utc), s.scrap_ts_utc)) AS iso_year,
                   DATEPART(ISO_WEEK, s.scrap_ts_utc) AS iso_week,
                   SUM(s.qty_units) AS scrap_units
            FROM mes.scrap_event s
            JOIN dim.line l ON l.line_id = s.line_id
            GROUP BY l.plant_id, s.line_id,
                     YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, s.scrap_ts_utc), s.scrap_ts_utc)),
                     DATEPART(ISO_WEEK, s.scrap_ts_utc)
        ),
        good AS (
            SELECT s.plant_id, s.line_id,
                   YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, CAST(s.production_day AS DATETIME)), CAST(s.production_day AS DATETIME))) AS iso_year,
                   DATEPART(ISO_WEEK, s.production_day) AS iso_week,
                   SUM(s.good_units) AS good_units
            FROM stg.production_local s
            GROUP BY s.plant_id, s.line_id,
                     YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, CAST(s.production_day AS DATETIME)), CAST(s.production_day AS DATETIME))),
                     DATEPART(ISO_WEEK, s.production_day)
        )
        INSERT INTO rpt.scrap_yield_weekly
        SELECT
            g.plant_id, g.line_id, g.iso_year, g.iso_week,
            g.good_units,
            ISNULL(s.scrap_units, 0) AS scrap_units,
            ROUND(CAST(ISNULL(s.scrap_units, 0) AS DECIMAL(18,4))
                  / NULLIF(g.good_units + ISNULL(s.scrap_units, 0), 0) * 100, 2) AS scrap_pct
        FROM good g
        LEFT JOIN scrap s ON s.plant_id = g.plant_id AND s.line_id = g.line_id
                         AND s.iso_year = g.iso_year AND s.iso_week = g.iso_week;

        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_scrap_yield_weekly',
             @@ROWCOUNT, 'OK';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_scrap_yield_weekly',
             NULL, 'FAILED', ERROR_MESSAGE();
        THROW;
    END CATCH
    RETURN 0;
END
GO
