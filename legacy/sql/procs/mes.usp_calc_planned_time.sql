-- =============================================================
-- mes.usp_calc_planned_time
-- Planned production minutes per line/shift = shift length minus
-- PLANNED downtime (PM, sanitation, changeover). Result lands in
-- stg.planned_time; consumed by rpt.usp_rpt_oee_shift.
-- History: 2016-04 initial; 2019-03 aligned to shift split rework.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_calc_planned_time
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.planned_time;
    CREATE TABLE stg.planned_time (
        plant_id         CHAR(5)     NOT NULL,
        line_id          VARCHAR(12) NOT NULL,
        production_day   DATE        NOT NULL,
        shift_code       VARCHAR(4)  NOT NULL,
        shift_minutes    INT         NOT NULL,
        planned_dt_min   INT         NOT NULL,
        unplanned_dt_min INT         NOT NULL
    );

    INSERT INTO stg.planned_time
    SELECT
        sc.plant_id,
        ln.line_id,
        sc.production_day,
        sc.shift_code,
        DATEDIFF(MINUTE, sc.start_utc, sc.end_utc) AS shift_minutes,
        ISNULL(SUM(CASE WHEN s.planned_flag = 1
                        THEN DATEDIFF(MINUTE, s.seg_start_local, s.seg_end_local) END), 0),
        ISNULL(SUM(CASE WHEN s.planned_flag = 0
                        THEN DATEDIFF(MINUTE, s.seg_start_local, s.seg_end_local) END), 0)
    FROM dim.shift_calendar sc
    JOIN dim.line ln          ON ln.plant_id = sc.plant_id
    LEFT JOIN stg.downtime_shift_seg s
           ON s.plant_id = sc.plant_id
          AND s.line_id  = ln.line_id
          AND s.production_day = sc.production_day
          AND s.shift_code = sc.shift_code
    GROUP BY sc.plant_id, ln.line_id, sc.production_day, sc.shift_code,
             sc.start_utc, sc.end_utc;
    RETURN 0;
END
GO
