-- Report output tables (the contract the lakehouse jobs must reproduce)
DROP TABLE IF EXISTS rpt.line_downtime_daily;
GO
CREATE TABLE rpt.line_downtime_daily (
    plant_id          CHAR(5)      NOT NULL,
    line_id           VARCHAR(12)  NOT NULL,
    production_day    DATE         NOT NULL,
    shift_code        VARCHAR(4)   NOT NULL,
    reason_category   VARCHAR(30)  NOT NULL,
    planned_flag      BIT          NOT NULL,
    event_count       INT          NOT NULL,
    downtime_minutes  INT          NOT NULL
);
GO
DROP TABLE IF EXISTS rpt.daily_production;
GO
CREATE TABLE rpt.daily_production (
    plant_id        CHAR(5)      NOT NULL,
    line_id         VARCHAR(12)  NOT NULL,
    production_day  DATE         NOT NULL,
    sku_id          VARCHAR(10)  NOT NULL,
    total_units     INT          NOT NULL,
    good_units      INT          NOT NULL,
    cases           INT          NULL,
    yield_pct       DECIMAL(9,2) NULL
);
GO
DROP TABLE IF EXISTS rpt.oee_shift;
GO
CREATE TABLE rpt.oee_shift (
    plant_id       CHAR(5)      NOT NULL,
    line_id        VARCHAR(12)  NOT NULL,
    production_day DATE         NOT NULL,
    shift_code     VARCHAR(4)   NOT NULL,
    planned_min    INT          NOT NULL,
    unplanned_dt_min INT        NOT NULL,
    availability   DECIMAL(9,4) NULL,
    performance    DECIMAL(9,4) NULL,
    quality        DECIMAL(9,4) NULL,
    oee            DECIMAL(9,4) NULL
);
GO
DROP TABLE IF EXISTS rpt.open_quality_holds;
GO
CREATE TABLE rpt.open_quality_holds (
    lot_id          VARCHAR(30)  NOT NULL,
    line_id         VARCHAR(12)  NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    status_ts_utc   DATETIME2(0) NOT NULL,
    open_minutes    INT          NOT NULL
);
GO
DROP TABLE IF EXISTS rpt.scrap_yield_weekly;
GO
CREATE TABLE rpt.scrap_yield_weekly (
    plant_id        CHAR(5)      NOT NULL,
    line_id         VARCHAR(12)  NOT NULL,
    iso_year        INT          NOT NULL,
    iso_week        INT          NOT NULL,
    good_units      INT          NOT NULL,
    scrap_units     INT          NOT NULL,
    scrap_pct       DECIMAL(9,2) NULL
);
GO
DROP TABLE IF EXISTS rpt.material_variance;
GO
CREATE TABLE rpt.material_variance (
    order_id        VARCHAR(14)  NOT NULL,
    material_code   VARCHAR(18)  NULL,
    std_qty         DECIMAL(14,2) NULL,
    actual_qty      DECIMAL(14,2) NULL,
    variance_qty    DECIMAL(14,2) NULL,
    variance_label  VARCHAR(40)  NULL
);
GO
