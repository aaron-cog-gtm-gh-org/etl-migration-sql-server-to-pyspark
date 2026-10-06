-- =============================================================
-- mes.usp_split_downtime_by_shift
-- Splits each localised downtime event at shift boundaries so minutes
-- attribute to (production_day, shift_code). Overlaps the event's
-- local span against dim.shift_calendar (local bounds derived from
-- the UTC calendar).
-- History: 2015-06 initial recursive CTE; 2019-03 RK - split at shift
-- boundaries per plant mgr request; 2021-11 moved to shift_calendar
-- overlap join for perf.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_split_downtime_by_shift
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.downtime_shift_seg;
    CREATE TABLE stg.downtime_shift_seg (
        event_id        INT          NOT NULL,
        plant_id        CHAR(5)      NOT NULL,
        line_id         VARCHAR(12)  NOT NULL,
        reason_code     VARCHAR(10)  NOT NULL,
        planned_flag    BIT          NOT NULL,
        production_day  DATE         NOT NULL,
        shift_code      VARCHAR(4)   NOT NULL,
        seg_start_local DATETIME2(0) NOT NULL,
        seg_end_local   DATETIME2(0) NOT NULL
    );

    INSERT INTO stg.downtime_shift_seg
    SELECT
        e.event_id,
        e.plant_id,
        e.line_id,
        e.reason_code,
        e.planned_flag,
        sc.production_day,
        sc.shift_code,
        CASE WHEN e.start_local > CAST(sc.start_utc AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0))
             THEN e.start_local
             ELSE CAST(sc.start_utc AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0)) END,
        CASE WHEN e.end_local   < CAST(sc.end_utc   AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0))
             THEN e.end_local
             ELSE CAST(sc.end_utc   AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0)) END
    FROM stg.downtime_local e
    JOIN dim.plant p          ON p.plant_id = e.plant_id
    JOIN dim.shift_calendar sc
         ON sc.plant_id = e.plant_id
        AND CAST(sc.start_utc AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0)) <  e.end_local
        AND CAST(sc.end_utc   AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0)) >  e.start_local;
    RETURN 0;
END
GO
