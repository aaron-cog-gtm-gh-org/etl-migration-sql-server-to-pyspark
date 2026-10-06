-- =============================================================
-- rpt.usp_rpt_oee_shift
-- OEE per line/shift: availability, performance, quality, oee.
-- Depends on PL_Line_Downtime outputs (stg.downtime_shift_seg via
-- stg.planned_time) and stg.production_local.
-- availability  = (shift_min - unplanned_dt) / shift_min
-- performance   = total_units / (ideal_rate * run_minutes)
-- quality       = good / total
-- All rounded to 4dp, NULLIF guards on zero denominators.
-- History: 2016-06 initial; 2021-02 exclude planned dt from A.
-- =============================================================
CREATE OR ALTER PROCEDURE rpt.usp_rpt_oee_shift
    @PipelineName VARCHAR(80) = 'PL_OEE'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId INT;
    EXEC etl.usp_log_run @RunId OUTPUT, @PipelineName, 'rpt.usp_rpt_oee_shift';

    BEGIN TRY
        TRUNCATE TABLE rpt.oee_shift;

        ;WITH prod AS (
            SELECT s.plant_id, s.line_id, s.production_day, s.shift_code,
                   SUM(s.total_units) AS total_units,
                   SUM(s.good_units)  AS good_units,
                   SUM(s.total_units) * 1.0 AS t
            FROM stg.production_local s
            GROUP BY s.plant_id, s.line_id, s.production_day, s.shift_code
        ),
        ideal AS (
            SELECT s.plant_id, s.line_id, s.production_day, s.shift_code,
                   SUM(k.ideal_units_per_min) AS ideal_rate
            FROM stg.production_local s
            JOIN dim.sku k ON k.sku_id = s.sku_id
            GROUP BY s.plant_id, s.line_id, s.production_day, s.shift_code
        )
        INSERT INTO rpt.oee_shift
        SELECT
            pt.plant_id, pt.line_id, pt.production_day, pt.shift_code,
            pt.shift_minutes AS planned_min,
            pt.unplanned_dt_min,
            ROUND(CAST(pt.shift_minutes - pt.unplanned_dt_min AS DECIMAL(18,4))
                  / NULLIF(pt.shift_minutes, 0), 4) AS availability,
            ROUND(CAST(pr.total_units AS DECIMAL(18,4))
                  / NULLIF(i.ideal_rate * (pt.shift_minutes - pt.unplanned_dt_min), 0), 4) AS performance,
            ROUND(CAST(pr.good_units AS DECIMAL(18,4))
                  / NULLIF(pr.total_units, 0), 4) AS quality,
            ROUND(
                CAST(pt.shift_minutes - pt.unplanned_dt_min AS DECIMAL(18,4)) / NULLIF(pt.shift_minutes, 0)
              * CAST(pr.total_units AS DECIMAL(18,4)) / NULLIF(i.ideal_rate * (pt.shift_minutes - pt.unplanned_dt_min), 0)
              * CAST(pr.good_units AS DECIMAL(18,4)) / NULLIF(pr.total_units, 0)
            , 4) AS oee
        FROM stg.planned_time pt
        JOIN prod  pr ON pr.plant_id = pt.plant_id AND pr.line_id = pt.line_id
                     AND pr.production_day = pt.production_day AND pr.shift_code = pt.shift_code
        JOIN ideal i  ON i.plant_id  = pt.plant_id AND i.line_id  = pt.line_id
                     AND i.production_day = pt.production_day AND i.shift_code  = pt.shift_code
        WHERE pt.production_day >= '2026-10-19';

        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_oee_shift',
             @@ROWCOUNT, 'OK';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_oee_shift',
             NULL, 'FAILED', ERROR_MESSAGE();
        THROW;
    END CATCH
    RETURN 0;
END
GO
