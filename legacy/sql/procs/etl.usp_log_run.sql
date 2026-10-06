-- etl.usp_log_run - writes a row to etl.run_log.
-- @RunId NULL opens a row; passing it back in closes it out.
CREATE OR ALTER PROCEDURE etl.usp_log_run
    @RunId        INT          = NULL OUTPUT,
    @PipelineName VARCHAR(80),
    @ProcName     VARCHAR(80),
    @RowsWritten  INT          = NULL,
    @Status       VARCHAR(12)  = 'RUNNING',
    @ErrMsg       NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @RunId IS NULL
    BEGIN
        INSERT INTO etl.run_log (pipeline_nm, proc_nm, started_utc, status, err_msg)
        VALUES (@PipelineName, @ProcName, SYSUTCDATETIME(), @Status, @ErrMsg);
        SET @RunId = SCOPE_IDENTITY();
    END
    ELSE
    BEGIN
        UPDATE etl.run_log
        SET finished_utc = SYSUTCDATETIME(),
            rows_written = @RowsWritten,
            status = @Status,
            err_msg = @ErrMsg
        WHERE run_id = @RunId;
    END
    RETURN 0;
END
GO
