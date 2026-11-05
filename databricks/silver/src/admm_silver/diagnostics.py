"""Exception metadata only: never serialize messages, rows, locals or full traces."""
from pathlib import Path
import re
from .contracts import SilverError
from .evidence import SAFE_VOLUME_ERRORS,SAFE_VOLUME_TYPES

PACKAGE=Path(__file__).resolve().parent


def failure_report(exc,stage):
    code='RUNTIME_FAILED'
    if isinstance(exc,SilverError) and re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',str(exc)):
        code=str(exc)
    location=None
    tb=exc.__traceback__
    while tb:
        path=Path(tb.tb_frame.f_code.co_filename).resolve()
        if path.is_relative_to(PACKAGE):
            location={'file':'admm_silver/'+path.relative_to(PACKAGE).as_posix(),'line':tb.tb_lineno}
        tb=tb.tb_next
    report={'status':'FAILED','stage':stage,'error_code':code,
            'error_type':type(exc).__name__,'location':location}
    cause=getattr(exc,'receipt_cause_type',None)
    provider=getattr(exc,'receipt_error_class',None)
    if isinstance(exc,SilverError) and isinstance(cause,str) and cause in SAFE_VOLUME_TYPES:
        report['cause_type']=cause
    if isinstance(exc,SilverError) and isinstance(provider,str) and provider in SAFE_VOLUME_ERRORS|{'UNCLASSIFIED'}:
        report['provider_error_class']=provider
    return report
