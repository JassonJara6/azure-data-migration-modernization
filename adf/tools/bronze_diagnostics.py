"""Structured diagnostics built from allowlisted facts, never raw exception text.

ODBC/HTTP messages can embed SQL, passwords, connection strings and data. We
inspect them only to classify known failures; no arbitrary substring is emitted.
"""
from contextlib import contextmanager
import re

SQL_CODES={18456,4060,229,208,2812,207,201,8144,1205,1222,2601,2627,10054,10060,10061,11001}
STATES={'08001','08004','08007','08S01','28000','42000','42S02','42S22','HYT00','HYT01','HY000','HY104','IM002','IM003','01000','40001','23000'}


# Exact, versioned messages only. Never echo an arbitrary ValueError/Arrow/JSON
# message: those can contain source rows, paths or secrets.
VALIDATION_REASONS={
 'Missing audit receipt':('AUDIT_RECEIPT_MISSING','Falta el recibo SQL del child.'),
 'Invalid Copy audit state':('COPY_AUDIT_INVALID','El estado o los conteos del recibo Copy no son válidos.'),
 'Attestation was not durable':('ATTESTATION_NOT_DURABLE','SQL no devolvió la atestación durable esperada.'),
 'Invalid durable parent receipt':('PARENT_RECEIPT_INVALID','El recibo durable no confirma exactamente once cargas del parent.'),
 'Expected eleven active sources, five incremental and six full':('SOURCE_CONTRACT_INVALID','Se requieren once fuentes activas: cinco INCREMENTAL y seis FULL.'),
 'Parent has missing, duplicate or unexpected source receipts':('PARENT_CHILDREN_INVALID','Faltan cargas del parent o existen recibos duplicados o inesperados.'),
 'ADF failed for another reason':('ADF_TERMINAL_FAILURE_INVALID','ADF falló por una causa distinta del bloqueo de confirmación esperado.'),
 'ADF children differ from SQL parent receipts':('ADF_CHILDREN_MISMATCH','Los once RunIds de ADF no coinciden con los recibos SQL.'),
 'Null or duplicate destination PK':('PARQUET_PK_INVALID','Parquet contiene claves primarias nulas o duplicadas.'),
 'Destination row outside bounded cursor':('PARQUET_CURSOR_OUT_OF_BOUNDS','Una fila Parquet queda fuera de la ventana lexicográfica capturada.'),
 'Destination does not reach captured upper tuple':('PARQUET_UPPER_TUPLE_MISSING','Parquet no alcanza la tupla superior capturada.'),
 'Destination count differs from plan':('PARQUET_COUNT_MISMATCH','El conteo Parquet no coincide con el plan.'),
 'Missing source/ingestion columns':('PARQUET_COLUMNS_MISSING','Faltan columnas fuente o de auditoría en Parquet.'),
 'Missing nonempty run folder':('ADLS_LANDING_MISSING','No existe la carpeta de una carga que esperaba filas.'),
 'Missing nonempty destination':('ADLS_LANDING_MISSING','No existen archivos de una carga que esperaba filas.'),
 'Destination changed before parent commit':('ADLS_DESTINATION_CHANGED','Los archivos cambiaron antes de confirmar el parent.'),
 'Destination mutated during reconciliation':('ADLS_DESTINATION_CHANGED','Los archivos cambiaron durante la reconciliación.'),
 'Destination byte size changed':('ADLS_DESTINATION_CHANGED','Cambió el tamaño de un archivo durante la descarga.'),
}


class BronzeFailure(Exception):
    def __init__(self,report):
        self.report=report
        super().__init__(report['code'])


def report_error(exc,stage='startup'):
    if isinstance(exc,BronzeFailure):return exc.report.copy()
    result={'status':'FAILED','stage':stage,'error_type':type(exc).__name__,
      'code':'BRONZE_OPERATION_FAILED','message':'La operación falló; no se imprimió el mensaje bruto para proteger los datos.',
      'hint':'Revisa la etapa indicada y la configuración del runtime.',
      'parent_commit_outcome':'UNKNOWN' if stage in ('sql_commit_parent','sql_verify_parent_receipt') else 'NOT_ATTEMPTED'}
    args=getattr(exc,'args',())
    # SQLSTATE is accepted only as its own five-character argument or [STATE].
    # Never infer it from arbitrary five-letter words in a credential/message.
    state=next((a for a in args if isinstance(a,str) and a in STATES),None)
    text=' '.join(a for a in args if isinstance(a,str))
    if state is None:
        state=next((s for s in re.findall(r'\[([A-Z0-9]{5})\]',text) if s in STATES),None)
    codes=[int(n) for n in re.findall(r'\((\d{1,6})\)',text) if int(n) in SQL_CODES or 51200<=int(n)<=51699]
    native=next((c for c in codes if c not in (10054,10060,10061,11001)),codes[0] if codes else None)
    lower=text.lower()
    if state:result['sqlstate']=state
    if native is not None:result['sql_native_code']=native
    def describe(code,message,hint):result.update(code=code,message=message,hint=hint)
    if native==18456 or state=='28000':
        describe('SQL_LOGIN_FAILED','SQL Server rechazó la autenticación.',
          'Comprueba el usuario y la contraseña. Usa --interactive para evitar expansión de Bash y escapar valores ODBC correctamente.')
    elif native==4060:
        describe('SQL_DATABASE_UNAVAILABLE','No se pudo abrir AdventureWorks con esta identidad.',
          'Comprueba que AdventureWorks exista y que el login tenga un usuario asociado en esa base.')
    elif state=='08001' and any(word in lower for word in ('certificate','certificado','ssl provider','tls')):
        describe('SQL_TLS_FAILED','ODBC no pudo validar la conexión TLS de SQL Server.',
          'Usa el certificado de confianza configurado. --trust-local-sql-certificate solo se permite para SQL de loopback.')
    elif state in ('08001','08004','08S01','08007') or native in (10054,10060,10061,11001):
        describe('SQL_CONNECTION_FAILED','Falló o se interrumpió la conexión con SQL Server.',
          'Comprueba el puerto de Docker y el servidor SQL del runtime Windows. Si ocurrió durante el commit, repite el mismo parent: se consultará el recibo durable.')
    elif state in ('HYT00','HYT01') or native==1222:
        describe('SQL_TIMEOUT','La conexión, consulta o espera de bloqueo agotó su tiempo.',
          'Revisa conectividad y bloqueos SQL. No reinicies cursores; reintenta el mismo parent si el resultado del commit es desconocido.')
    elif native==1205 or state=='40001':
        describe('SQL_DEADLOCK','SQL Server rechazó la transacción por concurrencia.',
          'Reintenta el mismo parent tras resolver la concurrencia; no recopies ni reinicies watermarks.')
    elif native==229:
        describe('SQL_PERMISSION_DENIED','La identidad SQL no tiene un permiso requerido en esta etapa.',
          'Revisa los grants mínimos del reconciliador en AdventureWorks; no concedas db_owner ni escritura directa.')
    elif native in (208,2812,207,201,8144) or state in ('42S02','42S22'):
        describe('SQL_CONTRACT_MISMATCH','El esquema o la firma de los objetos SQL no coincide con el código.',
          'Comprueba la base AdventureWorks y las migraciones/procedimientos desplegados; no falsifiques recibos ni evidencia.')
    elif native and 51200<=native<=51699:
        describe('SQL_GUARD_REJECTED','Un procedimiento rechazó la confirmación o evidencia.',
          'Consulta el número SQL en los procedimientos versionados. Conserva los archivos y el mismo parent; no fuerces el avance del cursor.')
    elif state in ('IM002','IM003') or (state=='01000' and any(word in lower for word in ("can't open lib",'cannot open shared object','could not be loaded'))):
        describe('ODBC_DRIVER_UNAVAILABLE','No se pudo cargar el driver ODBC solicitado.',
          'Comprueba ODBC Driver 18, sus dependencias nativas y que Python y el driver tengan la misma arquitectura.')
    elif state=='HY104':
        describe('SQL_PARAMETER_BINDING_FAILED','El driver rechazó la precisión o el tipo de un parámetro.',
          'Comprueba la versión pyodbc/ODBC y la firma SQL; la etapa identifica si falló la atestación o confirmación.')
    elif state:
        describe('SQL_OPERATION_FAILED','SQL Server/ODBC devolvió un error en la etapa indicada.',
          'Usa SQLSTATE y el código SQL controlado para diagnosticar; no publiques cadenas de conexión ni trazas sin filtrar.')
    elif type(exc) in (ValueError,RuntimeError) and len(args)==1 and isinstance(args[0],str) and args[0] in VALIDATION_REASONS:
        code,message=VALIDATION_REASONS[args[0]]
        describe(code,message,'Conserva los recibos y archivos originales; corrige la causa sin reiniciar ni forzar cursores.')
    elif isinstance(exc,(ValueError,KeyError,TypeError)):
        describe('BRONZE_VALIDATION_FAILED','Los datos o la configuración no cumplen el contrato Bronze.',
          'Revisa la etapa: metadatos, recibos, esquema Parquet, límites, conteos e inmutabilidad. No se mostraron valores de filas.')
    elif isinstance(exc,ImportError):
        describe('PYTHON_DEPENDENCY_MISSING','Falta una dependencia Python del reconciliador.',
          'Usa el entorno Python preparado con adf/tools/requirements-commit.txt.')
    else:
        status=getattr(exc,'status_code',None)
        if status is None:status=getattr(getattr(exc,'response',None),'status_code',None)
        if type(status) is int and status in (401,403,404,409,412,429,500,502,503,504):
            result['http_status']=status
            describe('AZURE_READ_FAILED','Falló una lectura o validación contra Azure.',
              'Comprueba identidad, permisos de lectura, fábrica/cuenta y ETags; no se mostraron URLs, tokens ni respuestas.')
        elif type(exc).__name__ in ('ClientAuthenticationError','CredentialUnavailableError','AuthenticationRequiredError'):
            describe('AZURE_AUTHENTICATION_FAILED','No hay una identidad Azure válida para la lectura.',
              'Usa la sesión Azure CLI existente o el inicio interactivo solicitado por --interactive.')
    return result


@contextmanager
def step(stage,**context):
    safe_context={k:v for k,v in context.items() if k in ('parent_run_id','child_run_id') and
                  isinstance(v,str) and re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',v)}
    try:yield
    except BronzeFailure as exc:
        exc.report.update(safe_context)
        if stage in ('sql_commit_parent','sql_verify_parent_receipt'):
            exc.report['parent_commit_outcome']='UNKNOWN'
        raise
    except Exception as exc:
        report=report_error(exc,stage);report.update(safe_context)
        raise BronzeFailure(report) from exc


def fail(stage,code,message,hint):
    raise BronzeFailure({'status':'FAILED','stage':stage,'error_type':'BronzeValidationError',
      'code':code,'message':message,'hint':hint,'parent_commit_outcome':'NOT_ATTEMPTED'})


def close_resources(*resources):
    # Cleanup must never replace a useful diagnostic, expose a raw exception,
    # or turn a verified durable receipt into an unhandled traceback.
    for resource in resources:
        if resource is not None:
            try:resource.close()
            except Exception:pass
