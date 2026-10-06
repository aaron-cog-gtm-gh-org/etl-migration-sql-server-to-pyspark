-- =============================================================
-- mes.usp_stg_downtime_local
-- Flattens mes.downtime_event into plant-local start/end for the
-- reporting window. Open events are capped at @AsOfUtc (a fixed
-- parameter so report output is deterministic).
-- History: 2015-03 initial; 2017-10 use AT TIME ZONE per plant row
-- instead of a hard-coded offset table.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_stg_downtime_local
    @AsOfUtc DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.downtime_local;
    CREATE TABLE stg.downtime_local (
        event_id      INT          NOT NULL,
        plant_id      CHAR(5)      NOT NULL,
        line_id       VARCHAR(12)  NOT NULL,
        reason_code   VARCHAR(10)  NOT NULL,
        planned_flag  BIT          NOT NULL,
        start_utc     DATETIME2(0) NOT NULL,
        end_utc       DATETIME2(0) NOT NULL,
        start_local   DATETIME2(0) NOT NULL,
        end_local     DATETIME2(0) NOT NULL
    );

    INSERT INTO stg.downtime_local
    SELECT
        e.event_id,
        l.plant_id,
        e.line_id,
        e.reason_code,
        e.planned_flag,
        e.start_utc,
        ISNULL(e.end_utc, @AsOfUtc) AS end_utc,
        CAST(e.start_utc AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0)),
        CAST(ISNULL(e.end_utc, @AsOfUtc) AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0))
    FROM mes.downtime_event e
    JOIN dim.line  l ON l.line_id = e.line_id
    JOIN dim.plant p ON p.plant_id = l.plant_id
    WHERE e.start_utc < @AsOfUtc;
    RETURN 0;
END
GO
