
CREATE OR ALTER PROCEDURE dbo.usp_log_pipeline_end
    @pipeline_name  VARCHAR(100),
    @status         VARCHAR(20),
    @rows_extracted INT,
    @rows_loaded    INT,
    @adf_run_id     VARCHAR(200),
    @error_message  NVARCHAR(MAX) = NULL
AS
BEGIN
    UPDATE dbo.pipeline_run_log
    SET
        end_time            = GETDATE(),
        duration_seconds    = DATEDIFF(
                                SECOND, start_time, GETDATE()
                              ),
        status              = @status,
        rows_extracted      = @rows_extracted,
        rows_loaded         = @rows_loaded,
        error_message       = @error_message
    WHERE
        adf_run_id      = @adf_run_id
    AND pipeline_name   = @pipeline_name;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_log_pipeline_start
    @pipeline_name  VARCHAR(100),
    @source_table   VARCHAR(100),
    @adf_run_id     VARCHAR(200),
    @adf_pipeline   VARCHAR(200),
    @adf_trigger    VARCHAR(200)
AS
BEGIN
    INSERT INTO dbo.pipeline_run_log (
        pipeline_name, source_table,
        start_time, status,
        adf_run_id, adf_pipeline_name, adf_trigger_name
    )
    VALUES (
        @pipeline_name, @source_table,
        GETDATE(), 'RUNNING',
        @adf_run_id, @adf_pipeline, @adf_trigger
    );
END;
GO
CREATE OR ALTER PROCEDURE dbo.usp_update_watermark
    @pipeline_name  VARCHAR(100),
    @source_table   VARCHAR(100),
    @new_watermark  VARCHAR(200),
    @rows_loaded    INT
AS
BEGIN
    UPDATE dbo.cdc_watermark
    SET
        last_watermark_value    = @new_watermark,
        last_watermark_datetime = GETDATE(),
        initial_load_done       = 1,
        last_successful_run     = GETDATE(),
        last_rows_loaded        = @rows_loaded,
        total_rows_loaded       = total_rows_loaded + @rows_loaded,
        modified_date           = GETDATE()
    WHERE
        pipeline_name = @pipeline_name
    AND source_table  = @source_table;
END;
GO;

-- ============================================================
-- SNOWFLAKE MERGE PROCEDURES
-- Purpose : Apply CDC changes from RAW → STAGING
-- Pattern : MERGE (UPSERT + SOFT DELETE)
-- ============================================================

USE DATABASE SUPPLY_CHAIN_DB;
USE SCHEMA STAGING;
USE WAREHOUSE SUPPLY_CHAIN_WH;
USE ROLE ACCOUNTADMIN;

-- ─────────────────────────────────────────────────────────
-- PROCEDURE 1: MERGE_SUPPLIERS
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SUPPLY_CHAIN_DB.STAGING.MERGE_SUPPLIERS(
    BATCH_ID    VARCHAR,
    RUN_ID      VARCHAR,
    PIPELINE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_updated    INTEGER DEFAULT 0;
    result_msg      VARCHAR;
BEGIN

    MERGE INTO SUPPLY_CHAIN_DB.STAGING.STG_SUPPLIERS AS TGT
    USING (
        -- Get latest record per supplier from RAW
        SELECT *
        FROM (
            SELECT
                SUPPLIER_ID,
                SUPPLIER_NAME,
                SUPPLIER_CODE,
                COUNTRY,
                CITY,
                CONTACT_EMAIL,
                CONTACT_PHONE,
                PAYMENT_TERMS,
                SUPPLIER_STATUS,
                RATING,
                CREATED_AT,
                UPDATED_AT,
                _PIPELINE_NAME,
                _BATCH_ID,
                _RUN_ID,
                _IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY SUPPLIER_ID
                    ORDER BY UPDATED_AT DESC,
                             _INGESTION_TIMESTAMP DESC
                ) AS RN
            FROM SUPPLY_CHAIN_DB.RAW.RAW_SUPPLIERS
            WHERE _BATCH_ID = :BATCH_ID
        )
        WHERE RN = 1
    ) AS SRC
    ON TGT.SUPPLIER_ID = SRC.SUPPLIER_ID

    -- UPDATE existing records
    WHEN MATCHED AND (
        TGT.UPDATED_AT < SRC.UPDATED_AT
        OR TGT._IS_DELETED != SRC._IS_DELETED
    ) THEN UPDATE SET
        TGT.SUPPLIER_NAME       = SRC.SUPPLIER_NAME,
        TGT.SUPPLIER_CODE       = SRC.SUPPLIER_CODE,
        TGT.COUNTRY             = SRC.COUNTRY,
        TGT.CITY                = SRC.CITY,
        TGT.CONTACT_EMAIL       = SRC.CONTACT_EMAIL,
        TGT.CONTACT_PHONE       = SRC.CONTACT_PHONE,
        TGT.PAYMENT_TERMS       = SRC.PAYMENT_TERMS,
        TGT.SUPPLIER_STATUS     = SRC.SUPPLIER_STATUS,
        TGT.RATING              = SRC.RATING,
        TGT.UPDATED_AT          = SRC.UPDATED_AT,
        TGT._CDC_OPERATION      = 'UPDATE',
        TGT._CDC_TIMESTAMP      = CURRENT_TIMESTAMP(),
        TGT._IS_DELETED         = SRC._IS_DELETED,
        TGT._IS_CURRENT         = TRUE,
        TGT._PIPELINE_NAME      = SRC._PIPELINE_NAME,
        TGT._BATCH_ID           = SRC._BATCH_ID,
        TGT._RUN_ID             = SRC._RUN_ID,
        TGT._UPDATED_AT         = CURRENT_TIMESTAMP()

    -- INSERT new records
    WHEN NOT MATCHED THEN INSERT (
        SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_CODE,
        COUNTRY, CITY, CONTACT_EMAIL, CONTACT_PHONE,
        PAYMENT_TERMS, SUPPLIER_STATUS, RATING,
        CREATED_AT, UPDATED_AT,
        _CDC_OPERATION, _CDC_TIMESTAMP,
        _IS_DELETED, _IS_CURRENT,
        _PIPELINE_NAME, _BATCH_ID, _RUN_ID,
        _CREATED_AT, _UPDATED_AT
    )
    VALUES (
        SRC.SUPPLIER_ID, SRC.SUPPLIER_NAME, SRC.SUPPLIER_CODE,
        SRC.COUNTRY, SRC.CITY, SRC.CONTACT_EMAIL, SRC.CONTACT_PHONE,
        SRC.PAYMENT_TERMS, SRC.SUPPLIER_STATUS, SRC.RATING,
        SRC.CREATED_AT, SRC.UPDATED_AT,
        'INSERT', CURRENT_TIMESTAMP(),
        SRC._IS_DELETED, TRUE,
        SRC._PIPELINE_NAME, SRC._BATCH_ID, SRC._RUN_ID,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    -- Capture row counts
    rows_inserted := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_SUPPLIERS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'INSERT');
    rows_updated  := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_SUPPLIERS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'UPDATE');

    result_msg := 'MERGE_SUPPLIERS COMPLETE | INSERTED: ' || rows_inserted
                  || ' | UPDATED: ' || rows_updated;
    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR IN MERGE_SUPPLIERS: ' || SQLERRM;
END;
$$;

-- ─────────────────────────────────────────────────────────
-- PROCEDURE 2: MERGE_PURCHASE_ORDERS
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SUPPLY_CHAIN_DB.STAGING.MERGE_PURCHASE_ORDERS(
    BATCH_ID        VARCHAR,
    RUN_ID          VARCHAR,
    PIPELINE_NAME   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_updated    INTEGER DEFAULT 0;
    result_msg      VARCHAR;
BEGIN

    MERGE INTO SUPPLY_CHAIN_DB.STAGING.STG_PURCHASE_ORDERS AS TGT
    USING (
        SELECT *
        FROM (
            SELECT
                PO_ID, PO_NUMBER, SUPPLIER_ID,
                ORDER_DATE, EXPECTED_DATE, ACTUAL_DATE,
                PO_STATUS, TOTAL_AMOUNT, CURRENCY,
                LINE_ITEMS_COUNT, WAREHOUSE_ID, NOTES,
                CREATED_AT, UPDATED_AT,
                _PIPELINE_NAME, _BATCH_ID, _RUN_ID, _IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY PO_ID
                    ORDER BY UPDATED_AT DESC,
                             _INGESTION_TIMESTAMP DESC
                ) AS RN
            FROM SUPPLY_CHAIN_DB.RAW.RAW_PURCHASE_ORDERS
            WHERE _BATCH_ID = :BATCH_ID
        )
        WHERE RN = 1
    ) AS SRC
    ON TGT.PO_ID = SRC.PO_ID

    WHEN MATCHED AND (
        TGT.UPDATED_AT < SRC.UPDATED_AT
        OR TGT._IS_DELETED != SRC._IS_DELETED
    ) THEN UPDATE SET
        TGT.PO_NUMBER           = SRC.PO_NUMBER,
        TGT.SUPPLIER_ID         = SRC.SUPPLIER_ID,
        TGT.ORDER_DATE          = SRC.ORDER_DATE,
        TGT.EXPECTED_DATE       = SRC.EXPECTED_DATE,
        TGT.ACTUAL_DATE         = SRC.ACTUAL_DATE,
        TGT.PO_STATUS           = SRC.PO_STATUS,
        TGT.TOTAL_AMOUNT        = SRC.TOTAL_AMOUNT,
        TGT.CURRENCY            = SRC.CURRENCY,
        TGT.LINE_ITEMS_COUNT    = SRC.LINE_ITEMS_COUNT,
        TGT.WAREHOUSE_ID        = SRC.WAREHOUSE_ID,
        TGT.NOTES               = SRC.NOTES,
        TGT.UPDATED_AT          = SRC.UPDATED_AT,
        TGT._CDC_OPERATION      = 'UPDATE',
        TGT._CDC_TIMESTAMP      = CURRENT_TIMESTAMP(),
        TGT._IS_DELETED         = SRC._IS_DELETED,
        TGT._IS_CURRENT         = TRUE,
        TGT._PIPELINE_NAME      = SRC._PIPELINE_NAME,
        TGT._BATCH_ID           = SRC._BATCH_ID,
        TGT._RUN_ID             = SRC._RUN_ID,
        TGT._UPDATED_AT         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        PO_ID, PO_NUMBER, SUPPLIER_ID,
        ORDER_DATE, EXPECTED_DATE, ACTUAL_DATE,
        PO_STATUS, TOTAL_AMOUNT, CURRENCY,
        LINE_ITEMS_COUNT, WAREHOUSE_ID, NOTES,
        CREATED_AT, UPDATED_AT,
        _CDC_OPERATION, _CDC_TIMESTAMP,
        _IS_DELETED, _IS_CURRENT,
        _PIPELINE_NAME, _BATCH_ID, _RUN_ID,
        _CREATED_AT, _UPDATED_AT
    )
    VALUES (
        SRC.PO_ID, SRC.PO_NUMBER, SRC.SUPPLIER_ID,
        SRC.ORDER_DATE, SRC.EXPECTED_DATE, SRC.ACTUAL_DATE,
        SRC.PO_STATUS, SRC.TOTAL_AMOUNT, SRC.CURRENCY,
        SRC.LINE_ITEMS_COUNT, SRC.WAREHOUSE_ID, SRC.NOTES,
        SRC.CREATED_AT, SRC.UPDATED_AT,
        'INSERT', CURRENT_TIMESTAMP(),
        SRC._IS_DELETED, TRUE,
        SRC._PIPELINE_NAME, SRC._BATCH_ID, SRC._RUN_ID,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    rows_inserted := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_PURCHASE_ORDERS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'INSERT');
    rows_updated  := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_PURCHASE_ORDERS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'UPDATE');

    result_msg := 'MERGE_PURCHASE_ORDERS COMPLETE | INSERTED: ' || rows_inserted
                  || ' | UPDATED: ' || rows_updated;
    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR IN MERGE_PURCHASE_ORDERS: ' || SQLERRM;
END;
$$;

-- ─────────────────────────────────────────────────────────
-- PROCEDURE 3: MERGE_INVENTORY
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SUPPLY_CHAIN_DB.STAGING.MERGE_INVENTORY(
    BATCH_ID        VARCHAR,
    RUN_ID          VARCHAR,
    PIPELINE_NAME   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_updated    INTEGER DEFAULT 0;
    result_msg      VARCHAR;
BEGIN

    MERGE INTO SUPPLY_CHAIN_DB.STAGING.STG_INVENTORY AS TGT
    USING (
        SELECT *
        FROM (
            SELECT
                INVENTORY_ID, PRODUCT_ID, PRODUCT_NAME,
                PRODUCT_SKU, WAREHOUSE_ID, WAREHOUSE_NAME,
                QUANTITY_ON_HAND, QUANTITY_RESERVED, QUANTITY_AVAILABLE,
                UNIT_COST, REORDER_POINT, REORDER_QUANTITY,
                LAST_COUNT_DATE, CREATED_AT, UPDATED_AT,
                _PIPELINE_NAME, _BATCH_ID, _RUN_ID, _IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY INVENTORY_ID
                    ORDER BY UPDATED_AT DESC,
                             _INGESTION_TIMESTAMP DESC
                ) AS RN
            FROM SUPPLY_CHAIN_DB.RAW.RAW_INVENTORY
            WHERE _BATCH_ID = :BATCH_ID
        )
        WHERE RN = 1
    ) AS SRC
    ON TGT.INVENTORY_ID = SRC.INVENTORY_ID

    WHEN MATCHED AND (
        TGT.UPDATED_AT < SRC.UPDATED_AT
        OR TGT._IS_DELETED != SRC._IS_DELETED
    ) THEN UPDATE SET
        TGT.PRODUCT_ID          = SRC.PRODUCT_ID,
        TGT.PRODUCT_NAME        = SRC.PRODUCT_NAME,
        TGT.PRODUCT_SKU         = SRC.PRODUCT_SKU,
        TGT.WAREHOUSE_ID        = SRC.WAREHOUSE_ID,
        TGT.WAREHOUSE_NAME      = SRC.WAREHOUSE_NAME,
        TGT.QUANTITY_ON_HAND    = SRC.QUANTITY_ON_HAND,
        TGT.QUANTITY_RESERVED   = SRC.QUANTITY_RESERVED,
        TGT.QUANTITY_AVAILABLE  = SRC.QUANTITY_AVAILABLE,
        TGT.UNIT_COST           = SRC.UNIT_COST,
        TGT.REORDER_POINT       = SRC.REORDER_POINT,
        TGT.REORDER_QUANTITY    = SRC.REORDER_QUANTITY,
        TGT.LAST_COUNT_DATE     = SRC.LAST_COUNT_DATE,
        TGT.UPDATED_AT          = SRC.UPDATED_AT,
        TGT._CDC_OPERATION      = 'UPDATE',
        TGT._CDC_TIMESTAMP      = CURRENT_TIMESTAMP(),
        TGT._IS_DELETED         = SRC._IS_DELETED,
        TGT._IS_CURRENT         = TRUE,
        TGT._PIPELINE_NAME      = SRC._PIPELINE_NAME,
        TGT._BATCH_ID           = SRC._BATCH_ID,
        TGT._RUN_ID             = SRC._RUN_ID,
        TGT._UPDATED_AT         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        INVENTORY_ID, PRODUCT_ID, PRODUCT_NAME,
        PRODUCT_SKU, WAREHOUSE_ID, WAREHOUSE_NAME,
        QUANTITY_ON_HAND, QUANTITY_RESERVED, QUANTITY_AVAILABLE,
        UNIT_COST, REORDER_POINT, REORDER_QUANTITY,
        LAST_COUNT_DATE, CREATED_AT, UPDATED_AT,
        _CDC_OPERATION, _CDC_TIMESTAMP,
        _IS_DELETED, _IS_CURRENT,
        _PIPELINE_NAME, _BATCH_ID, _RUN_ID,
        _CREATED_AT, _UPDATED_AT
    )
    VALUES (
        SRC.INVENTORY_ID, SRC.PRODUCT_ID, SRC.PRODUCT_NAME,
        SRC.PRODUCT_SKU, SRC.WAREHOUSE_ID, SRC.WAREHOUSE_NAME,
        SRC.QUANTITY_ON_HAND, SRC.QUANTITY_RESERVED, SRC.QUANTITY_AVAILABLE,
        SRC.UNIT_COST, SRC.REORDER_POINT, SRC.REORDER_QUANTITY,
        SRC.LAST_COUNT_DATE, SRC.CREATED_AT, SRC.UPDATED_AT,
        'INSERT', CURRENT_TIMESTAMP(),
        SRC._IS_DELETED, TRUE,
        SRC._PIPELINE_NAME, SRC._BATCH_ID, SRC._RUN_ID,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    rows_inserted := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_INVENTORY
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'INSERT');
    rows_updated  := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_INVENTORY
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'UPDATE');

    result_msg := 'MERGE_INVENTORY COMPLETE | INSERTED: ' || rows_inserted
                  || ' | UPDATED: ' || rows_updated;
    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR IN MERGE_INVENTORY: ' || SQLERRM;
END;
$$;

-- ─────────────────────────────────────────────────────────
-- PROCEDURE 4: MERGE_SHIPMENTS
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SUPPLY_CHAIN_DB.STAGING.MERGE_SHIPMENTS(
    BATCH_ID        VARCHAR,
    RUN_ID          VARCHAR,
    PIPELINE_NAME   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_updated    INTEGER DEFAULT 0;
    result_msg      VARCHAR;
BEGIN

    MERGE INTO SUPPLY_CHAIN_DB.STAGING.STG_SHIPMENTS AS TGT
    USING (
        SELECT *
        FROM (
            SELECT
                SHIPMENT_ID, SHIPMENT_NUMBER, PO_ID,
                CARRIER_NAME, TRACKING_NUMBER,
                ORIGIN_COUNTRY, DEST_COUNTRY,
                SHIP_DATE, EXPECTED_DATE, ACTUAL_DATE,
                SHIPMENT_STATUS, WEIGHT_KG,
                TOTAL_PIECES, FREIGHT_COST,
                CREATED_AT, UPDATED_AT,
                _PIPELINE_NAME, _BATCH_ID, _RUN_ID, _IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY SHIPMENT_ID
                    ORDER BY UPDATED_AT DESC,
                             _INGESTION_TIMESTAMP DESC
                ) AS RN
            FROM SUPPLY_CHAIN_DB.RAW.RAW_SHIPMENTS
            WHERE _BATCH_ID = :BATCH_ID
        )
        WHERE RN = 1
    ) AS SRC
    ON TGT.SHIPMENT_ID = SRC.SHIPMENT_ID

    WHEN MATCHED AND (
        TGT.UPDATED_AT < SRC.UPDATED_AT
        OR TGT._IS_DELETED != SRC._IS_DELETED
    ) THEN UPDATE SET
        TGT.SHIPMENT_NUMBER     = SRC.SHIPMENT_NUMBER,
        TGT.PO_ID               = SRC.PO_ID,
        TGT.CARRIER_NAME        = SRC.CARRIER_NAME,
        TGT.TRACKING_NUMBER     = SRC.TRACKING_NUMBER,
        TGT.ORIGIN_COUNTRY      = SRC.ORIGIN_COUNTRY,
        TGT.DEST_COUNTRY        = SRC.DEST_COUNTRY,
        TGT.SHIP_DATE           = SRC.SHIP_DATE,
        TGT.EXPECTED_DATE       = SRC.EXPECTED_DATE,
        TGT.ACTUAL_DATE         = SRC.ACTUAL_DATE,
        TGT.SHIPMENT_STATUS     = SRC.SHIPMENT_STATUS,
        TGT.WEIGHT_KG           = SRC.WEIGHT_KG,
        TGT.TOTAL_PIECES        = SRC.TOTAL_PIECES,
        TGT.FREIGHT_COST        = SRC.FREIGHT_COST,
        TGT.UPDATED_AT          = SRC.UPDATED_AT,
        TGT._CDC_OPERATION      = 'UPDATE',
        TGT._CDC_TIMESTAMP      = CURRENT_TIMESTAMP(),
        TGT._IS_DELETED         = SRC._IS_DELETED,
        TGT._IS_CURRENT         = TRUE,
        TGT._PIPELINE_NAME      = SRC._PIPELINE_NAME,
        TGT._BATCH_ID           = SRC._BATCH_ID,
        TGT._RUN_ID             = SRC._RUN_ID,
        TGT._UPDATED_AT         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        SHIPMENT_ID, SHIPMENT_NUMBER, PO_ID,
        CARRIER_NAME, TRACKING_NUMBER,
        ORIGIN_COUNTRY, DEST_COUNTRY,
        SHIP_DATE, EXPECTED_DATE, ACTUAL_DATE,
        SHIPMENT_STATUS, WEIGHT_KG,
        TOTAL_PIECES, FREIGHT_COST,
        CREATED_AT, UPDATED_AT,
        _CDC_OPERATION, _CDC_TIMESTAMP,
        _IS_DELETED, _IS_CURRENT,
        _PIPELINE_NAME, _BATCH_ID, _RUN_ID,
        _CREATED_AT, _UPDATED_AT
    )
    VALUES (
        SRC.SHIPMENT_ID, SRC.SHIPMENT_NUMBER, SRC.PO_ID,
        SRC.CARRIER_NAME, SRC.TRACKING_NUMBER,
        SRC.ORIGIN_COUNTRY, SRC.DEST_COUNTRY,
        SRC.SHIP_DATE, SRC.EXPECTED_DATE, SRC.ACTUAL_DATE,
        SRC.SHIPMENT_STATUS, SRC.WEIGHT_KG,
        SRC.TOTAL_PIECES, SRC.FREIGHT_COST,
        SRC.CREATED_AT, SRC.UPDATED_AT,
        'INSERT', CURRENT_TIMESTAMP(),
        SRC._IS_DELETED, TRUE,
        SRC._PIPELINE_NAME, SRC._BATCH_ID, SRC._RUN_ID,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    rows_inserted := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_SHIPMENTS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'INSERT');
    rows_updated  := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_SHIPMENTS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'UPDATE');

    result_msg := 'MERGE_SHIPMENTS COMPLETE | INSERTED: ' || rows_inserted
                  || ' | UPDATED: ' || rows_updated;
    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR IN MERGE_SHIPMENTS: ' || SQLERRM;
END;
$$;

-- ─────────────────────────────────────────────────────────
-- PROCEDURE 5: MERGE_SALES_ORDERS
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SUPPLY_CHAIN_DB.STAGING.MERGE_SALES_ORDERS(
    BATCH_ID        VARCHAR,
    RUN_ID          VARCHAR,
    PIPELINE_NAME   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_updated    INTEGER DEFAULT 0;
    result_msg      VARCHAR;
BEGIN

    MERGE INTO SUPPLY_CHAIN_DB.STAGING.STG_SALES_ORDERS AS TGT
    USING (
        SELECT *
        FROM (
            SELECT
                ORDER_ID, ORDER_NUMBER, CUSTOMER_ID,
                CUSTOMER_NAME, ORDER_DATE,
                SHIP_DATE, DELIVERY_DATE, ORDER_STATUS,
                ORDER_AMOUNT, DISCOUNT_AMOUNT,
                TAX_AMOUNT, TOTAL_AMOUNT, CURRENCY,
                SALES_CHANNEL, REGION,
                CREATED_AT, UPDATED_AT,
                _PIPELINE_NAME, _BATCH_ID, _RUN_ID, _IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY ORDER_ID
                    ORDER BY UPDATED_AT DESC,
                             _INGESTION_TIMESTAMP DESC
                ) AS RN
            FROM SUPPLY_CHAIN_DB.RAW.RAW_SALES_ORDERS
            WHERE _BATCH_ID = :BATCH_ID
        )
        WHERE RN = 1
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED AND (
        TGT.UPDATED_AT < SRC.UPDATED_AT
        OR TGT._IS_DELETED != SRC._IS_DELETED
    ) THEN UPDATE SET
        TGT.ORDER_NUMBER        = SRC.ORDER_NUMBER,
        TGT.CUSTOMER_ID         = SRC.CUSTOMER_ID,
        TGT.CUSTOMER_NAME       = SRC.CUSTOMER_NAME,
        TGT.ORDER_DATE          = SRC.ORDER_DATE,
        TGT.SHIP_DATE           = SRC.SHIP_DATE,
        TGT.DELIVERY_DATE       = SRC.DELIVERY_DATE,
        TGT.ORDER_STATUS        = SRC.ORDER_STATUS,
        TGT.ORDER_AMOUNT        = SRC.ORDER_AMOUNT,
        TGT.DISCOUNT_AMOUNT     = SRC.DISCOUNT_AMOUNT,
        TGT.TAX_AMOUNT          = SRC.TAX_AMOUNT,
        TGT.TOTAL_AMOUNT        = SRC.TOTAL_AMOUNT,
        TGT.CURRENCY            = SRC.CURRENCY,
        TGT.SALES_CHANNEL       = SRC.SALES_CHANNEL,
        TGT.REGION              = SRC.REGION,
        TGT.UPDATED_AT          = SRC.UPDATED_AT,
        TGT._CDC_OPERATION      = 'UPDATE',
        TGT._CDC_TIMESTAMP      = CURRENT_TIMESTAMP(),
        TGT._IS_DELETED         = SRC._IS_DELETED,
        TGT._IS_CURRENT         = TRUE,
        TGT._PIPELINE_NAME      = SRC._PIPELINE_NAME,
        TGT._BATCH_ID           = SRC._BATCH_ID,
        TGT._RUN_ID             = SRC._RUN_ID,
        TGT._UPDATED_AT         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, ORDER_NUMBER, CUSTOMER_ID,
        CUSTOMER_NAME, ORDER_DATE,
        SHIP_DATE, DELIVERY_DATE, ORDER_STATUS,
        ORDER_AMOUNT, DISCOUNT_AMOUNT,
        TAX_AMOUNT, TOTAL_AMOUNT, CURRENCY,
        SALES_CHANNEL, REGION,
        CREATED_AT, UPDATED_AT,
        _CDC_OPERATION, _CDC_TIMESTAMP,
        _IS_DELETED, _IS_CURRENT,
        _PIPELINE_NAME, _BATCH_ID, _RUN_ID,
        _CREATED_AT, _UPDATED_AT
    )
    VALUES (
        SRC.ORDER_ID, SRC.ORDER_NUMBER, SRC.CUSTOMER_ID,
        SRC.CUSTOMER_NAME, SRC.ORDER_DATE,
        SRC.SHIP_DATE, SRC.DELIVERY_DATE, SRC.ORDER_STATUS,
        SRC.ORDER_AMOUNT, SRC.DISCOUNT_AMOUNT,
        SRC.TAX_AMOUNT, SRC.TOTAL_AMOUNT, SRC.CURRENCY,
        SRC.SALES_CHANNEL, SRC.REGION,
        SRC.CREATED_AT, SRC.UPDATED_AT,
        'INSERT', CURRENT_TIMESTAMP(),
        SRC._IS_DELETED, TRUE,
        SRC._PIPELINE_NAME, SRC._BATCH_ID, SRC._RUN_ID,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    rows_inserted := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_SALES_ORDERS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'INSERT');
    rows_updated  := (SELECT COUNT(*) FROM SUPPLY_CHAIN_DB.STAGING.STG_SALES_ORDERS
                      WHERE _BATCH_ID = :BATCH_ID AND _CDC_OPERATION = 'UPDATE');

    result_msg := 'MERGE_SALES_ORDERS COMPLETE | INSERTED: ' || rows_inserted
                  || ' | UPDATED: ' || rows_updated;
    RETURN result_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR IN MERGE_SALES_ORDERS: ' || SQLERRM;
END;
$$;

-- ─────────────────────────────────────────────────────────
-- VERIFY ALL 5 PROCEDURES CREATED
-- ─────────────────────────────────────────────────────────
SHOW PROCEDURES IN SCHEMA SUPPLY_CHAIN_DB.STAGING;

-- ─────────────────────────────────────────────────────────
-- TEST CALL (after data lands in RAW)
-- ─────────────────────────────────────────────────────────
-- CALL SUPPLY_CHAIN_DB.STAGING.MERGE_SUPPLIERS(
--     'BATCH-001', 'RUN-001', 'PL_SUPPLIERS'
-- );
