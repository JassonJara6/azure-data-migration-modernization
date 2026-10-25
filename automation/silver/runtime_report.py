"""Project only known failure metadata; never expose provider error/trace bodies."""
import json
import re

CODES={'PACKAGE_VERSION_UNREADABLE','PACKAGE_VERSION_MISMATCH','PACKAGE_NOT_INSTALLED','RUNTIME_FAILED','RECEIPT_FILE_NOT_FOUND','RECEIPT_FILE_ACCESS_DENIED','RECEIPT_PATH_NOT_FILE',
 'RECEIPT_INVALID_UTF8','RECEIPT_FILE_IO_FAILED','RECEIPT_OBJECT_REQUIRED','RECEIPT_PARENT_REQUIRED',
 'REQUESTED_PARENT_MISMATCH','DUPLICATE_JSON_KEY','UNCOMMITTED_BUNDLE','MANIFEST_HASH_MISMATCH',
 'PARENT_HASH_MISMATCH','SOURCE_SET_MISMATCH','QUALITY_GATE_FAILED','REPLAY_CHANGED_DELTA'}
TYPES={'PackageNotFoundError','SilverError','RuntimeError','TypeError','ValueError','KeyError','FileNotFoundError','PermissionError',
 'OSError','UnicodeDecodeError','JSONDecodeError','AnalysisException','Py4JJavaError'}
STAGES={'package_preflight','configuration','unity_catalog_preflight','committed_evidence','silver_apply','idempotency_check'}


def safe_task_failure(output):
    result={'status':'FAILED','error_code':'UNAVAILABLE','stage':None,'error_type':None,'location':None}
    text=output.get('error','') if isinstance(output,dict) else ''
    if not isinstance(text,str):return result
    match=re.search(r'\bSILVER_([A-Z0-9_]{1,80})\b',text)
    if not match:return result
    result['error_code']=match[1] if match[1] in CODES else 'OTHER_SILVER_ERROR'
    start=text.find('{',match.end())
    if start<0:return result
    try:value=json.JSONDecoder().raw_decode(text[start:])[0]
    except ValueError:return result
    if not isinstance(value,dict):return result
    if isinstance(value.get('stage'),str) and value['stage'] in STAGES:result['stage']=value['stage']
    if isinstance(value.get('error_type'),str) and value['error_type'] in TYPES:result['error_type']=value['error_type']
    loc=value.get('location')
    if isinstance(loc,dict) and isinstance(loc.get('file'),str) and re.fullmatch(r'admm_silver/[a-z_]+\.py',loc['file']) and type(loc.get('line')) is int and 0<loc['line']<1000000:
        result['location']={'file':loc['file'],'line':loc['line']}
    return result
