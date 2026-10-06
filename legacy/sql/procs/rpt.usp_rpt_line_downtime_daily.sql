-- =============================================================
-- rpt.usp_rpt_line_downtime_daily
-- Rebuilds rpt.line_downtime_daily from stg.downtime_shift_seg.
-- Minutes are DATEDIFF(MINUTE, seg_start, seg_end): counts minute
-- boundaries crossed, not elapsed seconds / 60. event_count counts
-- distinct events touching the group.
-- History: 2015-07 initial; 2018-09 group by reason_category not
-- reason_code (FP request); 2022-04 added planned_flag to grain.
-- =============================================================
CREATE OR ALTER PROCEDURE rpt.usp_rpt_line_downtime_daily
    @PipelineName VARCHAR(80) = 'PL_Line_Downtime'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId INT;
    EXEC etl.usp_log_run @RunId OUTPUT, @PipelineName, 'rpt.usp_rpt_line_downtime_daily';

    BEGIN TRY
        TRUNCATE TABLE rpt.line_downtime_daily;

        INSERT INTO rpt.line_downtime_daily
        SELECT
            s.plant_id,
            s.line_id,
            s.production_day,
            s.shift_code,
            r.reason_category,
            s.planned_flag,
            COUNT(DISTINCT s.event_id)                            AS event_count,
            SUM(DATEDIFF(MINUTE, s.seg_start_local, s.seg_end_local)) AS downtime_minutes
        FROM stg.downtime_shift_seg s
        JOIN dim.downtime_reason r ON r.reason_code = s.reason_code
        GROUP BY s.plant_id, s.line_id, s.production_day, s.shift_code,
                 r.reason_category, s.planned_flag;

        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_line_downtime_daily',
             @@ROWCOUNT, 'OK';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_line_downtime_daily',
             NULL, 'FAILED', ERROR_MESSAGE();
        THROW;
    END CATCH
    RETURN 0;
END
GO
