-- Reference/dimension tables
DROP TABLE IF EXISTS dim.plant;
GO
CREATE TABLE dim.plant (
    plant_id       CHAR(5)      NOT NULL PRIMARY KEY,
    plant_name     NVARCHAR(100) NOT NULL,
    tz_name        NVARCHAR(64)  NOT NULL,   -- Windows tz name for AT TIME ZONE
    shift_pattern  VARCHAR(8)    NOT NULL
);
GO
DROP TABLE IF EXISTS dim.line;
GO
CREATE TABLE dim.line (
    line_id    VARCHAR(12)   NOT NULL PRIMARY KEY,
    plant_id   CHAR(5)       NOT NULL REFERENCES dim.plant(plant_id),
    line_name  NVARCHAR(50)  NOT NULL
);
GO
DROP TABLE IF EXISTS dim.shift_pattern;
GO
CREATE TABLE dim.shift_pattern (
    plant_id      CHAR(5)      NOT NULL REFERENCES dim.plant(plant_id),
    shift_code    VARCHAR(4)   NOT NULL,
    local_start   TIME         NOT NULL,
    local_end     TIME         NOT NULL,
    end_next_day  BIT          NOT NULL,
    CONSTRAINT PK_shift_pattern PRIMARY KEY (plant_id, shift_code)
);
GO
DROP TABLE IF EXISTS dim.calendar;
GO
CREATE TABLE dim.calendar (
    calendar_date DATE        NOT NULL PRIMARY KEY,
    iso_week      INT         NOT NULL,
    day_of_week   INT         NOT NULL,
    day_name      VARCHAR(12) NOT NULL
);
GO
DROP TABLE IF EXISTS dim.sku;
GO
CREATE TABLE dim.sku (
    sku_id               VARCHAR(10)   NOT NULL PRIMARY KEY,
    product_name         NVARCHAR(120) NOT NULL,
    pack_size            INT           NOT NULL,  -- each per case
    ideal_units_per_min  DECIMAL(9,2)  NOT NULL
);
GO
DROP TABLE IF EXISTS dim.downtime_reason;
GO
CREATE TABLE dim.downtime_reason (
    reason_code      VARCHAR(10)  NOT NULL PRIMARY KEY,
    reason_desc      NVARCHAR(120) NOT NULL,
    reason_category  VARCHAR(30)   NOT NULL,
    planned_default  BIT           NOT NULL
);
GO
DROP TABLE IF EXISTS dim.material_standard;
GO
CREATE TABLE dim.material_standard (
    order_id          VARCHAR(14)  NOT NULL,
    material_code     CHAR(18)     NOT NULL,
    std_qty_per_unit  DECIMAL(12,3) NOT NULL,
    CONSTRAINT PK_material_standard PRIMARY KEY (order_id, material_code)
);
GO
-- Materialised shift calendar in UTC; refreshed by dim.usp_refresh_shift_calendar.
DROP TABLE IF EXISTS dim.shift_calendar;
GO
CREATE TABLE dim.shift_calendar (
    plant_id        CHAR(5)     NOT NULL,
    shift_code      VARCHAR(4)  NOT NULL,
    production_day  DATE        NOT NULL,
    start_utc       DATETIME2(0) NOT NULL,
    end_utc         DATETIME2(0) NOT NULL,
    CONSTRAINT PK_shift_calendar PRIMARY KEY (plant_id, shift_code, production_day)
);
GO
