SET NOCOUNT ON;

IF DB_ID(N'AdventureWorks') IS NOT NULL
BEGIN
    PRINT 'AdventureWorks already exists; skipping restore.';
    RETURN;
END;

DECLARE @backup_file nvarchar(4000) = N'$(BackupFile)';
DECLARE @files table
(
    LogicalName nvarchar(128), PhysicalName nvarchar(260), [Type] char(1), FileGroupName nvarchar(128) NULL,
    Size numeric(20,0), MaxSize numeric(20,0), FileId bigint, CreateLSN numeric(25,0), DropLSN numeric(25,0) NULL,
    UniqueId uniqueidentifier, ReadOnlyLSN numeric(25,0) NULL, ReadWriteLSN numeric(25,0) NULL,
    BackupSizeInBytes bigint, SourceBlockSize int, FileGroupId int, LogGroupGUID uniqueidentifier NULL,
    DifferentialBaseLSN numeric(25,0) NULL, DifferentialBaseGUID uniqueidentifier NULL,
    IsReadOnly bit, IsPresent bit, TDEThumbprint varbinary(32) NULL, SnapshotUrl nvarchar(360) NULL
);

INSERT @files EXEC (N'RESTORE FILELISTONLY FROM DISK = ''' + @backup_file + N'''');

DECLARE @data_logical sysname = (SELECT TOP (1) LogicalName FROM @files WHERE [Type] = 'D' ORDER BY FileId);
DECLARE @log_logical sysname = (SELECT TOP (1) LogicalName FROM @files WHERE [Type] = 'L' ORDER BY FileId);
IF @data_logical IS NULL OR @log_logical IS NULL THROW 50001, 'Backup data or log file was not found.', 1;

DECLARE @restore nvarchar(max) =
    N'RESTORE DATABASE [AdventureWorks] FROM DISK = ' + QUOTENAME(@backup_file, '''') +
    N' WITH MOVE ' + QUOTENAME(@data_logical, '''') + N' TO ''/var/opt/mssql/data/AdventureWorks.mdf'',' +
    N' MOVE ' + QUOTENAME(@log_logical, '''') + N' TO ''/var/opt/mssql/data/AdventureWorks_log.ldf'', RECOVERY, STATS = 10;';
EXEC sys.sp_executesql @restore;

ALTER DATABASE [AdventureWorks] SET RECOVERY SIMPLE;
PRINT 'AdventureWorks restore completed.';
