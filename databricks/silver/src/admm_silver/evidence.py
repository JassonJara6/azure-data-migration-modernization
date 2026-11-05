"""Read complete committed evidence using the configured execution identity."""
import re
from .contracts import require,strict_json,SilverError

MAX_RECEIPT_BYTES=10*1024*1024
SAFE_VOLUME_ERRORS=frozenset(('PATH_NOT_FOUND','FILE_NOT_FOUND','INSUFFICIENT_PERMISSIONS',
    'PERMISSION_DENIED','INSUFFICIENT_PRIVILEGES','CATALOG_NOT_FOUND','SCHEMA_NOT_FOUND',
    'UC_VOLUME_NOT_FOUND','CREDENTIAL_NOT_FOUND','EXTERNAL_LOCATION_NOT_FOUND'))
SAFE_VOLUME_TYPES=frozenset(('AnalysisException','Py4JJavaError','SparkConnectGrpcException',
    'PySparkException','PermissionError','FileNotFoundError','OSError','RuntimeError','Exception'))


def _spark_bytes(spark,path):
    """DataFrame file access avoids dependence on the driver's POSIX/FUSE mount.

    Exact validated volume filename only. No glob, recursive search, RDD, SDK
    credentials, local copy, truncating head(), or fallback to another identity.
    """
    from pyspark.sql import functions as F
    try:
        rows=(spark.read.format('binaryFile').load(path)
              .select('length',F.when(F.col('length')<=MAX_RECEIPT_BYTES,F.col('content')).alias('content'))
              .take(2))
    except Exception as exc:
        # Only structured provider codes are inspected; messages may contain
        # paths, tokens or row data. Unknown errors keep a distinct safe code.
        error_class=None
        getter=getattr(exc,'getErrorClass',None)
        if callable(getter):
            try:error_class=getter()
            except Exception:pass
        code='RECEIPT_VOLUME_READ_FAILED'
        if error_class in ('PATH_NOT_FOUND','FILE_NOT_FOUND'):code='RECEIPT_VOLUME_FILE_NOT_FOUND'
        if error_class in ('INSUFFICIENT_PERMISSIONS','PERMISSION_DENIED','INSUFFICIENT_PRIVILEGES'):
            code='RECEIPT_FILE_ACCESS_DENIED'
        failure=SilverError(code)
        failure.receipt_cause_type=type(exc).__name__ if type(exc).__name__ in SAFE_VOLUME_TYPES else 'Exception'
        failure.receipt_error_class=error_class if isinstance(error_class,str) and error_class in SAFE_VOLUME_ERRORS else 'UNCLASSIFIED'
        raise failure from None
    require(len(rows)==1,'RECEIPT_SINGLE_FILE_REQUIRED')
    row=rows[0]
    require(type(row['length']) is int and 0<row['length']<=MAX_RECEIPT_BYTES,'RECEIPT_SIZE_INVALID')
    content=row['content']
    require(isinstance(content,(bytes,bytearray)) and len(content)==row['length'],'RECEIPT_INCOMPLETE_READ')
    return bytes(content)


def load_committed_evidence(receipt_file,parent,*,spark=None):
    if spark is not None:
        # Validate before Spark interprets any path/glob syntax. The filename is
        # bound to the requested parent; volume/catalog are explicit parameters.
        require(isinstance(parent,str) and re.fullmatch(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}',parent),
                'RECEIPT_PARENT_INVALID')
        require(isinstance(receipt_file,str) and re.fullmatch(
            r'/Volumes/[A-Za-z_][A-Za-z0-9_]*/[A-Za-z_][A-Za-z0-9_]*/[A-Za-z_][A-Za-z0-9_]*/'+re.escape(parent)+r'\.json',receipt_file),
            'RECEIPT_VOLUME_PATH_REQUIRED')
        raw=_spark_bytes(spark,'dbfs:'+receipt_file)
        try:text=raw.decode('utf-8',errors='strict')
        except UnicodeError:raise SilverError('RECEIPT_INVALID_UTF8') from None
    else:
        # Local validation uses regular files. Databricks must select Spark;
        # never silently retry a failed Volume read through local open().
        require(not str(receipt_file).startswith(('/Volumes/','dbfs:')),'RECEIPT_SPARK_READER_REQUIRED')
        try:
            with open(receipt_file,encoding='utf-8') as stream:
                text=stream.read()
        except FileNotFoundError:
            raise SilverError('RECEIPT_FILE_NOT_FOUND') from None
        except PermissionError:
            raise SilverError('RECEIPT_FILE_ACCESS_DENIED') from None
        except IsADirectoryError:
            raise SilverError('RECEIPT_PATH_NOT_FILE') from None
        except UnicodeError:
            raise SilverError('RECEIPT_INVALID_UTF8') from None
        except OSError:
            raise SilverError('RECEIPT_FILE_IO_FAILED') from None
    bundle=strict_json(text)
    require(isinstance(bundle,dict),'RECEIPT_OBJECT_REQUIRED')
    require(isinstance(bundle.get('parent_run_id'),str),'RECEIPT_PARENT_REQUIRED')
    require(bundle['parent_run_id']==parent,'REQUESTED_PARENT_MISMATCH')
    # Full hashes/11-table validation remains in pipeline.run before publication.
    return bundle
