-- Raw MES feed tables (stg.* holds transformed staging rows)
DROP TABLE IF EXISTS mes.production_count;
GO
CREATE TABLE mes.production_count (
    line_id          VARCHAR(12)  NOT NULL,
    bucket_start_utc DATETIME2(0) NOT NULL,
    bucket_end_utc   DATETIME2(0) NOT NULL,
    sku_id           VARCHAR(10)  NOT NULL,
    total_units      INT          NOT NULL,
    good_units       INT          NOT NULL
);
GO
DROP TABLE IF EXISTS mes.downtime_event;
GO
CREATE TABLE mes.downtime_event (
    event_id     INT           NOT NULL PRIMARY KEY,
    line_id      VARCHAR(12)   NOT NULL,
    start_utc    DATETIME2(0)  NOT NULL,
    end_utc      DATETIME2(0)  NULL,          -- NULL = still open
    reason_code  VARCHAR(10)   NOT NULL,
    planned_flag BIT           NOT NULL
);
GO
DROP TABLE IF EXISTS mes.line_speed_sample;
GO
CREATE TABLE mes.line_speed_sample (
    sample_id      INT           NOT NULL PRIMARY KEY,
    line_id        VARCHAR(12)   NOT NULL,
    sample_ts_utc  DATETIME2(0)  NOT NULL,
    units_per_min  DECIMAL(9,1)  NOT NULL
);
GO
DROP TABLE IF EXISTS mes.production_order;
GO
CREATE TABLE mes.production_order (
    order_id        VARCHAR(14)  NOT NULL PRIMARY KEY,
    line_id         VARCHAR(12)  NOT NULL,
    plant_id        CHAR(5)      NOT NULL,
    sku_id          VARCHAR(10)  NOT NULL,
    sched_start_utc DATETIME2(0) NOT NULL,
    sched_end_utc   DATETIME2(0) NOT NULL,
    planned_qty     INT          NOT NULL
);
GO
DROP TABLE IF EXISTS mes.scrap_event;
GO
CREATE TABLE mes.scrap_event (
    scrap_id    INT           NOT NULL PRIMARY KEY,
    line_id     VARCHAR(12)   NOT NULL,
    scrap_ts_utc DATETIME2(0) NOT NULL,
    qty_units   INT           NOT NULL,
    scrap_type  VARCHAR(40)   NOT NULL
);
GO
DROP TABLE IF EXISTS mes.quality_sample;
GO
CREATE TABLE mes.quality_sample (
    sample_id     INT          NOT NULL PRIMARY KEY,
    lot_id        VARCHAR(30)  NOT NULL,
    line_id       VARCHAR(12)  NOT NULL,
    sample_ts_utc DATETIME2(0) NOT NULL,
    result        VARCHAR(8)   NOT NULL,
    hold_flag     CHAR(1)      NOT NULL
);
GO
DROP TABLE IF EXISTS mes.quality_hold_event;
GO
CREATE TABLE mes.quality_hold_event (
    event_id      INT          NOT NULL PRIMARY KEY,
    lot_id        VARCHAR(30)  NOT NULL,
    status        VARCHAR(20)  NOT NULL,
    status_ts_utc DATETIME2(0) NOT NULL
);
GO
DROP TABLE IF EXISTS mes.material_movement;
GO
CREATE TABLE mes.material_movement (
    movement_id   INT          NOT NULL PRIMARY KEY,
    order_id      VARCHAR(14)  NOT NULL,
    material_code CHAR(18)     NOT NULL,   -- some feeds pad with trailing spaces
    movement_type VARCHAR(8)   NOT NULL,   -- ISSUE / RETURN
    quantity      DECIMAL(14,2) NOT NULL,
    ts_utc        DATETIME2(0) NOT NULL
);
GO
