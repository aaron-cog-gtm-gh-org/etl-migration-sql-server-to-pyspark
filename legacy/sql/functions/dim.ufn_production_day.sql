-- =============================================================
-- dim.ufn_production_day
-- Returns the production day (date) for a LOCAL datetime.
-- Production day runs 06:00 local -> 06:00 local next day, labelled
-- by the date the 06:00 anchor falls on.
-- History: 2014-11 first cut (DBA); 2018-05 JM - bugfix, day labelled
-- by anchor date not end date.
-- =============================================================
CREATE OR ALTER FUNCTION dim.ufn_production_day (@local_dt DATETIME2(0))
RETURNS DATE
AS
BEGIN
    RETURN CAST(DATEADD(HOUR, -6, CAST(@local_dt AS DATETIME)) AS DATE);
END
GO
