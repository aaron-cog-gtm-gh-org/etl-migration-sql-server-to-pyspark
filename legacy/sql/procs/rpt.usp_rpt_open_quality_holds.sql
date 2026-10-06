-- =============================================================
-- rpt.usp_rpt_open_quality_holds
-- Lots whose latest hold status is ON_HOLD as of @AsOfUtc.
-- open_minutes = DATEDIFF(MINUTE, status_ts, @AsOfUtc).
-- History: 2017-08 initial.
-- =============================================================
CREATE OR ALTER PROCEDURE rpt.usp_rpt_open_quality_holds
    @AsOfUtc      DATETIME2(0),
    @PipelineName VARCHAR(80) = 'PL_Quality_Holds'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId INT;
    EXEC etl.usp_log_run @RunId OUTPUT, @PipelineName, 'rpt.usp_rpt_open_quality_holds';

    BEGIN TRY
        TRUNCATE TABLE rpt.open_quality_holds;

        INSERT INTO rpt.open_quality_holds
        SELECT
            h.lot_id,
            q.line_id,
            h.status,
            h.status_ts_utc,
            DATEDIFF(MINUTE, h.status_ts_utc, @AsOfUtc) AS open_minutes
        FROM stg.lot_hold_status h
        JOIN stg.quality_samples q ON q.lot_id = h.lot_id
        WHERE h.status = 'ON_HOLD';

        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_open_quality_holds',
             @@ROWCOUNT, 'OK';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_open_quality_holds',
             NULL, 'FAILED', ERROR_MESSAGE();
        THROW;
    END CATCH
    RETURN 0;
END
GO
