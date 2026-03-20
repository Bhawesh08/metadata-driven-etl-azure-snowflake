
CREATE TABLE dbo.pipeline_config (
    pipeline_id             INT           IDENTITY(1,1)  PRIMARY KEY,
    pipeline_name           VARCHAR(100)  NOT NULL,
    source_type             VARCHAR(50)   NOT NULL,
    source_server           VARCHAR(200)  NULL,
    source_database         VARCHAR(100)  NULL,
    source_schema           VARCHAR(50)   NULL,
    source_table            VARCHAR(100)  NOT NULL,
    source_query            NVARCHAR(MAX) NULL,
    load_type               VARCHAR(20)   NOT NULL,
    cdc_column              VARCHAR(100)  NULL,
    cdc_column_type         VARCHAR(20)   NULL,
    primary_key_columns     VARCHAR(200)  NULL,
    sink_schema             VARCHAR(50)   NOT NULL,
    sink_table              VARCHAR(100)  NOT NULL,
    adls_container          VARCHAR(100)  NOT NULL,
    adls_folder_path        VARCHAR(500)  NOT NULL,
    file_format             VARCHAR(20)   NOT NULL DEFAULT 'PARQUET',
    batch_size              INT           NULL DEFAULT 100000,
    dq_enabled              BIT           NOT NULL DEFAULT 1,
    is_active               BIT           NOT NULL DEFAULT 1,
    retry_count             INT           NOT NULL DEFAULT 3,
    parallel_copy           BIT           NOT NULL DEFAULT 0,
    depends_on_pipeline     VARCHAR(100)  NULL,
    schedule_frequency      VARCHAR(20)   NULL,
    schedule_interval       INT           NULL DEFAULT 1,
    created_date            DATETIME      NOT NULL DEFAULT GETDATE(),
    modified_date           DATETIME      NOT NULL DEFAULT GETDATE(),
    created_by              VARCHAR(100)  NULL DEFAULT 'SYSTEM',

    CONSTRAINT uq_pipeline_name UNIQUE (pipeline_name),
    CONSTRAINT chk_load_type    CHECK  (load_type   IN ('FULL','CDC','INCREMENTAL')),
    CONSTRAINT chk_source_type  CHECK  (source_type IN ('SQLSERVER','CSV','API','PARQUET'))
);
GO

CREATE INDEX ix_pipeline_config_active
    ON dbo.pipeline_config (is_active, pipeline_id);
GO;

CREATE TABLE dbo.cdc_watermark (
    watermark_id              INT           IDENTITY(1,1) PRIMARY KEY,
    pipeline_name             VARCHAR(100)  NOT NULL,
    source_table              VARCHAR(100)  NOT NULL,
    cdc_column                VARCHAR(100)  NOT NULL,
    last_watermark_value      NVARCHAR(200) NULL,
    last_watermark_datetime   DATETIME      NULL,
    initial_load_done         BIT           NOT NULL DEFAULT 0,
    last_successful_run       DATETIME      NULL,
    last_rows_loaded          INT           NULL DEFAULT 0,
    total_rows_loaded         BIGINT        NULL DEFAULT 0,
    created_date              DATETIME      NOT NULL DEFAULT GETDATE(),
    modified_date             DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT uq_watermark UNIQUE (pipeline_name, source_table)
);
GO

CREATE INDEX ix_cdc_watermark_pipeline
    ON dbo.cdc_watermark (pipeline_name, source_table);
GO;

CREATE TABLE dbo.dq_rules (
    rule_id              INT           IDENTITY(1,1) PRIMARY KEY,
    pipeline_name        VARCHAR(100)  NOT NULL,
    rule_name            VARCHAR(100)  NOT NULL,
    rule_type            VARCHAR(50)   NOT NULL,
    column_name          VARCHAR(100)  NULL,
    rule_expression      NVARCHAR(MAX) NULL,
    min_value            VARCHAR(100)  NULL,
    max_value            VARCHAR(100)  NULL,
    regex_pattern        VARCHAR(500)  NULL,
    expected_value       VARCHAR(200)  NULL,
    threshold_pct        DECIMAL(5,2)  NULL,
    ref_table            VARCHAR(200)  NULL,
    ref_column           VARCHAR(100)  NULL,
    action_on_failure    VARCHAR(20)   NOT NULL DEFAULT 'QUARANTINE',
    severity             VARCHAR(10)   NOT NULL DEFAULT 'HIGH',
    error_message        VARCHAR(500)  NULL,
    rule_order           INT           NULL DEFAULT 1,
    is_active            BIT           NOT NULL DEFAULT 1,
    created_date         DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT uq_dq_rule  UNIQUE (pipeline_name, rule_name),
    CONSTRAINT chk_rule_type CHECK (
        rule_type IN (
            'NOT_NULL','UNIQUENESS','RANGE',
            'REGEX','REFERENTIAL','THRESHOLD','CUSTOM'
        )
    ),
    CONSTRAINT chk_action CHECK (
        action_on_failure IN ('QUARANTINE','REJECT','WARN','FLAG')
    ),
    CONSTRAINT chk_severity CHECK (
        severity IN ('HIGH','MEDIUM','LOW')
    )
);
GO

CREATE INDEX ix_dq_rules_pipeline
    ON dbo.dq_rules (pipeline_name, is_active);
GO;

CREATE TABLE dbo.pipeline_run_log (
    run_id               INT           IDENTITY(1,1) PRIMARY KEY,
    pipeline_name        VARCHAR(100)  NOT NULL,
    source_table         VARCHAR(100)  NOT NULL,
    run_date             DATE          NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    start_time           DATETIME      NOT NULL DEFAULT GETDATE(),
    end_time             DATETIME      NULL,
    duration_seconds     INT           NULL,
    status               VARCHAR(20)   NOT NULL DEFAULT 'RUNNING',
    rows_extracted       INT           NULL DEFAULT 0,
    rows_passed_dq       INT           NULL DEFAULT 0,
    rows_failed_dq       INT           NULL DEFAULT 0,
    rows_loaded          INT           NULL DEFAULT 0,
    watermark_start      NVARCHAR(200) NULL,
    watermark_end        NVARCHAR(200) NULL,
    adf_run_id           VARCHAR(200)  NULL,
    adf_pipeline_name    VARCHAR(200)  NULL,
    adf_trigger_name     VARCHAR(200)  NULL,
    error_message        NVARCHAR(MAX) NULL,
    created_date         DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT chk_run_status CHECK (
        status IN ('RUNNING','SUCCESS','FAILED','PARTIAL','SKIPPED')
    )
);
GO

CREATE INDEX ix_run_log_pipeline_date
    ON dbo.pipeline_run_log (pipeline_name, run_date, status);
GO;

CREATE TABLE dbo.dq_results (
    dq_result_id         INT           IDENTITY(1,1) PRIMARY KEY,
    run_id               INT           NULL,
    pipeline_name        VARCHAR(100)  NOT NULL,
    source_table         VARCHAR(100)  NOT NULL,
    rule_id              INT           NULL,
    rule_name            VARCHAR(100)  NOT NULL,
    rule_type            VARCHAR(50)   NOT NULL,
    column_name          VARCHAR(100)  NULL,
    total_records        INT           NULL DEFAULT 0,
    passed_records       INT           NULL DEFAULT 0,
    failed_records       INT           NULL DEFAULT 0,
    pass_rate_pct        DECIMAL(5,2)  NULL,
    status               VARCHAR(10)   NOT NULL,
    action_taken         VARCHAR(20)   NULL,
    dq_file_path         VARCHAR(500)  NULL,
    run_date             DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT fk_dq_results_rule    
        FOREIGN KEY (rule_id) 
        REFERENCES dbo.dq_rules(rule_id),
    CONSTRAINT fk_dq_results_run     
        FOREIGN KEY (run_id)  
        REFERENCES dbo.pipeline_run_log(run_id),
    CONSTRAINT chk_dq_status 
        CHECK (status IN ('PASS','FAIL','WARN'))
);
GO

CREATE INDEX ix_dq_results_pipeline
    ON dbo.dq_results (pipeline_name, run_date);
GO
;

CREATE TABLE dbo.error_log (
    error_id             INT           IDENTITY(1,1) PRIMARY KEY,
    run_id               INT           NULL,
    pipeline_name        VARCHAR(100)  NOT NULL,
    source_table         VARCHAR(100)  NULL,
    error_type           VARCHAR(50)   NULL,
    error_message        NVARCHAR(MAX) NULL,
    error_details        NVARCHAR(MAX) NULL,
    adf_activity_name    VARCHAR(200)  NULL,
    adf_run_id           VARCHAR(200)  NULL,
    is_resolved          BIT           NOT NULL DEFAULT 0,
    resolved_by          VARCHAR(100)  NULL,
    resolved_date        DATETIME      NULL,
    resolution_notes     NVARCHAR(MAX) NULL,
    created_date         DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT fk_error_log_run  
        FOREIGN KEY (run_id) 
        REFERENCES dbo.pipeline_run_log(run_id),
    CONSTRAINT chk_error_type CHECK (
        error_type IN (
            'EXTRACTION','TRANSFORMATION','LOAD',
            'DQ','WATERMARK','CONNECTION','UNKNOWN'
        )
    )
);
GO

CREATE INDEX ix_error_log_pipeline
    ON dbo.error_log (pipeline_name, is_resolved, created_date);
GO
;



SELECT 
    TABLE_NAME,
    CREATE_DATE = GETDATE()
FROM 
    INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_SCHEMA = 'dbo'
AND TABLE_TYPE  = 'BASE TABLE'
ORDER BY 
    TABLE_NAME;
