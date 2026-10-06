-- =============================================================
-- mes.usp_stg_quality_samples
-- Stages quality samples joined to line/plant for the holds report.
-- History: 2016-01 initial.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_stg_quality_samples
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.quality_samples;
    CREATE TABLE stg.quality_samples (
        lot_id        VARCHAR(30)  NOT NULL,
        line_id       VARCHAR(12)  NOT NULL,
        plant_id      CHAR(5)      NOT NULL,
        last_sample_utc DATETIME2(0) NOT NULL,
        fail_count    INT          NOT NULL
    );

    INSERT INTO stg.quality_samples
    SELECT
        q.lot_id,
        q.line_id,
        l.plant_id,
        MAX(q.sample_ts_utc),
        SUM(CASE WHEN q.result = 'FAIL' THEN 1 ELSE 0 END)
    FROM mes.quality_sample q
    JOIN dim.line l ON l.line_id = q.line_id
    GROUP BY q.lot_id, q.line_id, l.plant_id;
    RETURN 0;
END
GO
