-- ETL bookkeeping
DROP TABLE IF EXISTS etl.run_log;
GO
CREATE TABLE etl.run_log (
    run_id       INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_nm  VARCHAR(80)   NOT NULL,
    proc_nm      VARCHAR(80)   NOT NULL,
    started_utc  DATETIME2(0)  NOT NULL,
    finished_utc DATETIME2(0)  NULL,
    rows_written INT           NULL,
    status       VARCHAR(12)   NOT NULL,
    err_msg      NVARCHAR(500) NULL
);
GO
