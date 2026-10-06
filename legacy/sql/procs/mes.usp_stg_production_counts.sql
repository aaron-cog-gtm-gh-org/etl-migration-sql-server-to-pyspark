-- =============================================================
-- mes.usp_stg_production_counts
-- Localises mes.production_count buckets and tags each bucket with
-- its production day via dim.ufn_production_day (06:00 local anchor).
-- A bucket straddling 06:00 is attributed by its START time.
-- History: 2014-12 initial; 2017-02 attribute by bucket start not end.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_stg_production_counts
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.production_local;
    CREATE TABLE stg.production_local (
        line_id          VARCHAR(12)  NOT NULL,
        plant_id         CHAR(5)      NOT NULL,
        sku_id           VARCHAR(10)  NOT NULL,
        production_day   DATE         NOT NULL,
        shift_code       VARCHAR(4)   NOT NULL,
        bucket_minutes   INT          NOT NULL,
        total_units      INT          NOT NULL,
        good_units       INT          NOT NULL
    );

    INSERT INTO stg.production_local
    SELECT
        c.line_id,
        l.plant_id,
        c.sku_id,
        dim.ufn_production_day(
            CAST(c.bucket_start_utc AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name AS DATETIME2(0))),
        sc.shift_code,
        DATEDIFF(MINUTE, c.bucket_start_utc, c.bucket_end_utc) AS bucket_minutes,
        c.total_units,
        c.good_units
    FROM mes.production_count c
    JOIN dim.line  l ON l.line_id = c.line_id
    JOIN dim.plant p ON p.plant_id = l.plant_id
    JOIN dim.shift_calendar sc
         ON sc.plant_id = l.plant_id
        AND c.bucket_start_utc >= sc.start_utc
        AND c.bucket_start_utc <  sc.end_utc;
    RETURN 0;
END
GO
