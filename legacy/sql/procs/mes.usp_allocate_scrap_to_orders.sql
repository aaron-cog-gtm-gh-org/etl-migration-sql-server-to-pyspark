-- =============================================================
-- mes.usp_allocate_scrap_to_orders
-- Allocates each line scrap event to the production orders that
-- overlap it, proportional to overlap minutes; the rounding
-- remainder goes to the order with the largest overlap.
-- Output: stg.scrap_alloc(order_id, scrap_id, qty_units).
-- History: 2015-09 initial cursor (KE); 2021-06 kept cursor, set-based
-- attempt broke remainder rule.
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_allocate_scrap_to_orders
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.scrap_alloc;
    CREATE TABLE stg.scrap_alloc (
        scrap_id   INT          NOT NULL,
        order_id   VARCHAR(14)  NOT NULL,
        qty_units  INT          NOT NULL
    );

    DECLARE @scrap_id INT, @line_id VARCHAR(12), @ts DATETIME2(0), @qty INT;

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT scrap_id, line_id, scrap_ts_utc, qty_units
        FROM mes.scrap_event ORDER BY scrap_id;

    OPEN cur;
    FETCH NEXT FROM cur INTO @scrap_id, @line_id, @ts, @qty;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- proportional allocation over overlapping orders on the same line
        DROP TABLE IF EXISTS #ov;
        SELECT o.order_id,
               DATEDIFF(SECOND,
                   CASE WHEN o.sched_start_utc > @ts - 7200 THEN o.sched_start_utc ELSE @ts - 7200 END,
                   CASE WHEN o.sched_end_utc   < @ts + 7200 THEN o.sched_end_utc   ELSE @ts + 7200 END
               ) AS ov_secs
        INTO #ov
        FROM mes.production_order o
        WHERE o.line_id = @line_id
          AND o.sched_start_utc < DATEADD(SECOND, 7200, @ts)
          AND o.sched_end_utc   > DATEADD(SECOND, -7200, @ts)
          AND @ts BETWEEN o.sched_start_utc AND o.sched_end_utc;

        IF EXISTS (SELECT 1 FROM #ov)
        BEGIN
            DECLARE @tot INT = (SELECT SUM(ov_secs) FROM #ov);
            INSERT INTO stg.scrap_alloc
            SELECT @scrap_id, order_id,
                   CAST(@qty * ov_secs * 1.0 / @tot AS INT)  -- floor per order
            FROM #ov;
            -- remainder to the order with the largest overlap
            UPDATE t
            SET t.qty_units = t.qty_units +
                (@qty - (SELECT SUM(qty_units) FROM stg.scrap_alloc WHERE scrap_id = @scrap_id))
            FROM stg.scrap_alloc t
            JOIN (SELECT TOP 1 order_id FROM #ov ORDER BY ov_secs DESC, order_id) big
              ON big.order_id = t.order_id
            WHERE t.scrap_id = @scrap_id;
        END
        ELSE
            INSERT INTO stg.scrap_alloc (scrap_id, order_id, qty_units)
            VALUES (@scrap_id, 'UNALLOCATED', @qty);

        FETCH NEXT FROM cur INTO @scrap_id, @line_id, @ts, @qty;
    END
    CLOSE cur; DEALLOCATE cur;
    RETURN 0;
END
GO
