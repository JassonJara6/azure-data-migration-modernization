-- Transactional commit gate for independently reconciled Bronze runs.
-- Requires a separately privileged reconciler to persist audit evidence.
-- Trusted external coordinator only; ADF cannot attest or commit.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE ingestion.commit_bronze_run
    @child_run_id nvarchar(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF DB_NAME() <> N'AdventureWorks'
        THROW 51210, 'Incorrect database.', 1;
    IF NULLIF(LTRIM(RTRIM(@child_run_id)), N'') IS NULL
        THROW 51211, 'A child run ID is required.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @config_id int, @mode varchar(11), @status varchar(20),
                @expected bigint, @copied bigint, @reconciled bigint,
                @lower datetime2(7), @upper datetime2(7),
                @lower_keys nvarchar(1000), @upper_keys nvarchar(1000),
                @expected_version varbinary(8), @actual_version varbinary(8),
                @current_lower datetime2(7), @current_keys nvarchar(1000),
                @pk nvarchar(400), @k1 sysname, @k2 sysname,
                @l1 int, @l2 int, @u1 int, @u2 int;

        SELECT @config_id=config_id, @mode=load_type, @status=status,
               @expected=expected_rows, @copied=copied_rows,
               @reconciled=reconciled_rows, @lower=watermark_lower,
               @upper=watermark_upper, @lower_keys=watermark_lower_key_json,
               @upper_keys=watermark_upper_key_json,
               @expected_version=expected_row_version
        FROM ingestion.bronze_run_audit WITH (UPDLOCK, HOLDLOCK)
        WHERE child_run_id=@child_run_id;

        IF @status='COMMITTED'
        BEGIN
            COMMIT TRANSACTION;
            RETURN; -- Exact immutable run replay, even after newer runs advance.
        END;
        IF @config_id IS NULL OR @status <> 'RECONCILED'
            THROW 51212, 'Run does not have durable reconciled status.', 1;
        IF NOT EXISTS(SELECT 1 FROM ingestion.bronze_run_audit WHERE child_run_id=@child_run_id
                      AND manifest_sha256 IS NOT NULL AND manifest_json IS NOT NULL
                      AND reconciled_at_utc IS NOT NULL AND attested_by IS NOT NULL)
            THROW 51226, 'Independent reconciliation manifest is missing.', 1;
        IF @expected IS NULL OR @copied IS NULL OR @reconciled IS NULL
           OR @expected<>@copied OR @expected<>@reconciled
            THROW 51213, 'Reconciled and copied row counts must match plan.', 1;

        SELECT @pk=primary_key_columns
        FROM ingestion.table_config WITH (HOLDLOCK)
        WHERE config_id=@config_id AND load_type=@mode AND is_active=1;
        IF @pk IS NULL OR NOT EXISTS
        (SELECT 1 FROM ingestion.bronze_run_audit AS a JOIN ingestion.table_config AS c ON c.config_id=a.config_id
         WHERE a.child_run_id=@child_run_id AND a.primary_key_columns COLLATE Latin1_General_100_BIN2 =
               c.primary_key_columns COLLATE Latin1_General_100_BIN2
           AND ISNULL(a.watermark_column,N'') COLLATE Latin1_General_100_BIN2 =
               ISNULL(c.watermark_column,N'') COLLATE Latin1_General_100_BIN2
           AND LEFT(a.bronze_path,LEN(c.bronze_relative_path)+1)=c.bronze_relative_path+N'/')
            THROW 51214, 'Source configuration changed or was disabled.', 1;

        IF @mode='INCREMENTAL'
        BEGIN
            SELECT @current_lower=watermark_value,
                   @current_keys=watermark_key_json,
                   @actual_version=row_version
            FROM ingestion.watermark_state WITH (UPDLOCK, HOLDLOCK)
            WHERE config_id=@config_id;
            IF @actual_version IS NULL OR @expected_version IS NULL
               OR @actual_version<>@expected_version
                THROW 51215, 'Stale or missing watermark rowversion.', 1;
            IF @lower IS NULL OR @upper IS NULL OR @lower<>@current_lower
               OR ISNULL(ISJSON(@lower_keys),0)<>1 OR ISNULL(ISJSON(@current_keys),0)<>1
               OR ISNULL(ISJSON(@upper_keys),0)<>1
                THROW 51216, 'Invalid or stale cursor boundary.', 1;

            SET @k1=JSON_VALUE(@pk,'$[0]');
            SET @k2=JSON_VALUE(@pk,'$[1]');
            IF @k1 IS NULL OR JSON_VALUE(@pk,'$[2]') IS NOT NULL
                THROW 51217, 'Unsupported key contract.', 1;

            IF LEFT(LTRIM(@lower_keys),1)<>N'{' OR LEFT(LTRIM(@upper_keys),1)<>N'{'
               OR LEFT(LTRIM(@current_keys),1)<>N'{'
               OR EXISTS(SELECT [key] FROM OPENJSON(@lower_keys) GROUP BY [key] HAVING COUNT(*)>1)
               OR EXISTS(SELECT [key] FROM OPENJSON(@upper_keys) GROUP BY [key] HAVING COUNT(*)>1)
               OR EXISTS(SELECT [key] FROM OPENJSON(@current_keys) GROUP BY [key] HAVING COUNT(*)>1)
               OR EXISTS(SELECT [key],[value],[type] FROM OPENJSON(@lower_keys)
                         EXCEPT SELECT [key],[value],[type] FROM OPENJSON(@current_keys))
               OR EXISTS(SELECT [key],[value],[type] FROM OPENJSON(@current_keys)
                         EXCEPT SELECT [key],[value],[type] FROM OPENJSON(@lower_keys))
                THROW 51219, 'Lower cursor key object differs from committed state.', 1;
            -- Validate exact key set and integer values, independent of JSON property order.
            IF (SELECT COUNT(*) FROM OPENJSON(@lower_keys)) <>
               (SELECT COUNT(*) FROM OPENJSON(@pk))
               OR (SELECT COUNT(*) FROM OPENJSON(@upper_keys)) <>
                  (SELECT COUNT(*) FROM OPENJSON(@pk))
               OR EXISTS (SELECT 1 FROM OPENJSON(@lower_keys) AS j
                          WHERE j.[type]<>2 OR TRY_CONVERT(int,j.[value]) IS NULL
                             OR j.[value] LIKE '%[^0-9]%'
                             OR NOT EXISTS(SELECT 1 FROM OPENJSON(@pk) AS p
                               WHERE p.[value] COLLATE Latin1_General_100_BIN2 =
                                     j.[key] COLLATE Latin1_General_100_BIN2))
               OR EXISTS (SELECT 1 FROM OPENJSON(@upper_keys) AS j
                          WHERE j.[type]<>2 OR TRY_CONVERT(int,j.[value]) IS NULL
                             OR j.[value] LIKE '%[^0-9]%'
                             OR NOT EXISTS(SELECT 1 FROM OPENJSON(@pk) AS p
                               WHERE p.[value] COLLATE Latin1_General_100_BIN2 =
                                     j.[key] COLLATE Latin1_General_100_BIN2))
                THROW 51218, 'Invalid cursor key contract.', 1;

            SET @l1=TRY_CONVERT(int,JSON_VALUE(@lower_keys, '$."' + @k1 + '"'));
            SET @u1=TRY_CONVERT(int,JSON_VALUE(@upper_keys, '$."' + @k1 + '"'));
            SET @l2=CASE WHEN @k2 IS NULL THEN 0 ELSE
                TRY_CONVERT(int,JSON_VALUE(@lower_keys, '$."' + @k2 + '"')) END;
            SET @u2=CASE WHEN @k2 IS NULL THEN 0 ELSE
                TRY_CONVERT(int,JSON_VALUE(@upper_keys, '$."' + @k2 + '"')) END;
            IF @l1 IS NULL OR @u1 IS NULL OR @l2 IS NULL OR @u2 IS NULL
               OR @l1<0 OR @l2<0 OR @u1<0 OR @u2<0
               OR @l1<>TRY_CONVERT(int,JSON_VALUE(@current_keys, '$."' + @k1 + '"'))
               OR @l2<>CASE WHEN @k2 IS NULL THEN 0 ELSE
                   TRY_CONVERT(int,JSON_VALUE(@current_keys, '$."' + @k2 + '"')) END
                THROW 51219, 'Lower cursor does not match committed state.', 1;

            IF @upper<@lower OR
               (@upper=@lower AND
                (@u1<@l1 OR (@u1=@l1 AND @u2<@l2)))
                THROW 51220, 'Watermark would move backwards.', 1;
            IF @expected=0 AND
               (@upper<>@lower OR @u1<>@l1 OR @u2<>@l2)
                THROW 51221, 'Empty incremental batch cannot advance cursor.', 1;
            -- Nonempty batches must advance their bounded cursor; otherwise
            -- a malformed audit row could falsely confirm skipped rows.
            IF @expected>0 AND @upper=@lower AND @u1=@l1 AND @u2=@l2
                THROW 51225, 'Nonempty incremental batch has no cursor progress.', 1;

            UPDATE ingestion.watermark_state
            SET watermark_value=@upper,
                watermark_key_json=@upper_keys,
                last_successful_run_id=@child_run_id,
                last_successful_at_utc=SYSUTCDATETIME(),
                last_successful_row_count=@reconciled
            WHERE config_id=@config_id AND row_version=@expected_version;
            IF @@ROWCOUNT<>1
                THROW 51222, 'Optimistic concurrency update failed.', 1;
        END
        ELSE IF @mode<>'FULL'
            THROW 51223, 'Unsupported load type.', 1;

        UPDATE ingestion.bronze_run_audit
        SET status='COMMITTED', committed_at_utc=SYSUTCDATETIME(), updated_at_utc=SYSUTCDATETIME()
        WHERE child_run_id=@child_run_id AND status='RECONCILED';
        IF @@ROWCOUNT<>1
            THROW 51224, 'Audit commit transition failed.', 1;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE()<>0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO
-- No GRANT EXECUTE by default: trusted reconciliation writer and permissions
-- must be established before this procedure can be invoked.
