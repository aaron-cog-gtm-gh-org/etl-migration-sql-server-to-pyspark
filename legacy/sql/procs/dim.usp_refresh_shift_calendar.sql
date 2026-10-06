-- =============================================================
-- dim.usp_refresh_shift_calendar
-- Rebuilds dim.shift_calendar (plant, shift, production_day -> UTC bounds)
-- for the requested date window. Shift bounds are defined in plant LOCAL
-- time and converted to UTC with AT TIME ZONE (Windows tz names on
-- dim.plant). On the fall-back day the night shift is 25h long.
-- History: 2015-02 initial; 2020-08 RK - generate via calendar join
-- instead of cursor; 2023-01 PLT04 note - Mexico dropped DST.
-- =============================================================
CREATE OR ALTER PROCEDURE dim.usp_refresh_shift_calendar
    @StartDate DATE,
    @Days      INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @EndDate DATE = DATEADD(DAY, @Days - 1, @StartDate);

    DELETE sc
    FROM dim.shift_calendar sc
    WHERE sc.production_day BETWEEN @StartDate AND @EndDate;

    INSERT INTO dim.shift_calendar (plant_id, shift_code, production_day, start_utc, end_utc)
    SELECT
        p.plant_id,
        sp.shift_code,
        c.calendar_date AS production_day,
        CAST(CAST(DATEADD(SECOND, DATEDIFF(SECOND, '00:00', sp.local_start), CAST(c.calendar_date AS DATETIME)) AS DATETIME2(0))
             AT TIME ZONE p.tz_name AT TIME ZONE 'UTC' AS DATETIME2(0)) AS start_utc,
        CAST(CAST(DATEADD(DAY, sp.end_next_day,
                DATEADD(SECOND, DATEDIFF(SECOND, '00:00', sp.local_end), CAST(c.calendar_date AS DATETIME))) AS DATETIME2(0))
             AT TIME ZONE p.tz_name AT TIME ZONE 'UTC' AS DATETIME2(0)) AS end_utc
    FROM dim.plant p
    JOIN dim.shift_pattern sp ON sp.plant_id = p.plant_id
    JOIN dim.calendar c        ON c.calendar_date BETWEEN @StartDate AND @EndDate;
    RETURN 0;
END
GO
