-- =============================================================
-- mes.usp_merge_quality_hold_status
-- MERGE-upserts the latest hold status per lot into
-- stg.lot_hold_status. Latest = ROW_NUMBER() OVER (PARTITION BY lot
-- ORDER BY status_ts DESC, event_id DESC): on duplicate timestamps
-- the higher event_id wins (later feed record). Lots with no feed
-- rows since last run keep prior status (WHEN NOT MATCHED BY SOURCE
-- deliberately absent).
-- History: 2017-05 initial; 2020-03 tie-break on event_id after
-- duplicate-ts feed bug; 2023-09 added current_flag.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_merge_quality_hold_status
    @AsOfUtc DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    IF OBJECT_ID('stg.lot_hold_status') IS NULL
        CREATE TABLE stg.lot_hold_status (
            lot_id        VARCHAR(30)  NOT NULL PRIMARY KEY,
            status        VARCHAR(20)  NOT NULL,
            status_ts_utc DATETIME2(0) NOT NULL,
            event_id      INT          NOT NULL
        );

    ;WITH latest AS (
        SELECT lot_id, status, status_ts_utc, event_id
        FROM (
            SELECT h.*,
                   ROW_NUMBER() OVER (PARTITION BY h.lot_id
                                      ORDER BY h.status_ts_utc DESC, h.event_id DESC) AS rn
            FROM mes.quality_hold_event h
            WHERE h.status_ts_utc <= @AsOfUtc
        ) x WHERE rn = 1
    )
    MERGE stg.lot_hold_status AS t
    USING latest AS s ON t.lot_id = s.lot_id
    WHEN MATCHED AND (t.status <> s.status OR t.status_ts_utc <> s.status_ts_utc)
        THEN UPDATE SET t.status = s.status, t.status_ts_utc = s.status_ts_utc,
                        t.event_id = s.event_id
    WHEN NOT MATCHED THEN
        INSERT (lot_id, status, status_ts_utc, event_id)
        VALUES (s.lot_id, s.status, s.status_ts_utc, s.event_id);
    RETURN 0;
END
GO
