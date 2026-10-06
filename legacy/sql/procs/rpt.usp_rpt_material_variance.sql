-- =============================================================
-- rpt.usp_rpt_material_variance
-- Standard vs actual material qty per order.
-- Join on material_code: CHAR comparison ignores trailing spaces.
-- variance_label built with + concatenation: NULL if any part NULL
-- (legacy behaviour, kept for downstream consumers).
-- History: 2016-09 initial; 2019-12 label concat for ERP extract.
-- =============================================================
CREATE OR ALTER PROCEDURE rpt.usp_rpt_material_variance
    @PipelineName VARCHAR(80) = 'PL_Material_Consumption'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId INT;
    EXEC etl.usp_log_run @RunId OUTPUT, @PipelineName, 'rpt.usp_rpt_material_variance';

    BEGIN TRY
        TRUNCATE TABLE rpt.material_variance;

        INSERT INTO rpt.material_variance
        SELECT
            COALESCE(n.order_id, s.order_id) AS order_id,
            RTRIM(COALESCE(n.material_code, s.material_code)) AS material_code,
            s.std_qty_per_unit * o.planned_qty AS std_qty,
            n.net_qty AS actual_qty,
            n.net_qty - s.std_qty_per_unit * o.planned_qty AS variance_qty,
            o.sku_id + '/' + RTRIM(COALESCE(n.material_code, s.material_code))
                + ':' + CONVERT(VARCHAR(20), CAST(n.net_qty - s.std_qty_per_unit * o.planned_qty AS DECIMAL(14,2)))
                AS variance_label
        FROM stg.material_net n
        FULL OUTER JOIN dim.material_standard s
             ON s.order_id = n.order_id AND s.material_code = n.material_code
        LEFT JOIN mes.production_order o
             ON o.order_id = COALESCE(n.order_id, s.order_id);

        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_material_variance',
             @@ROWCOUNT, 'OK';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_log_run @RunId, @PipelineName, 'rpt.usp_rpt_material_variance',
             NULL, 'FAILED', ERROR_MESSAGE();
        THROW;
    END CATCH
    RETURN 0;
END
GO
