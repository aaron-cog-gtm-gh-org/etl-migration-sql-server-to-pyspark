-- =============================================================
-- mes.usp_stg_material_movements
-- Nets ISSUE minus RETURN per order/material into stg.material_net.
-- Material codes come through as CHAR(18); joins/comparisons
-- downstream rely on server collation trailing-space semantics.
-- History: 2015-11 initial; 2018-04 RETURNs netted (stores request).
-- =============================================================
CREATE OR ALTER PROCEDURE mes.usp_stg_material_movements
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS stg.material_net;
    CREATE TABLE stg.material_net (
        order_id      VARCHAR(14)   NOT NULL,
        material_code CHAR(18)      NOT NULL,
        net_qty       DECIMAL(14,2) NOT NULL
    );

    INSERT INTO stg.material_net
    SELECT m.order_id, m.material_code,
           SUM(CASE WHEN m.movement_type = 'ISSUE'  THEN m.quantity
                    WHEN m.movement_type = 'RETURN' THEN -m.quantity
                    ELSE 0 END)
    FROM mes.material_movement m
    GROUP BY m.order_id, m.material_code;
    RETURN 0;
END
GO
