"""Exact Volume path contracts plus real local Spark binaryFile execution."""
import json
from types import SimpleNamespace
import pytest
from admm_silver.contracts import SilverError
from admm_silver import evidence

PARENT='20d65511-8190-41a0-8ad2-23d7568363d2'
PATH='/Volumes/admm_dev/inputs/receipts/'+PARENT+'.json'

@pytest.mark.parametrize('path',[PATH+'/*',PATH+' ',PATH.replace('/inputs/','/inputs/../'),
    'dbfs:'+PATH,PATH.replace(PARENT,'other'),PATH.replace('/receipts/','/receipts*/'),'/tmp/'+PARENT+'.json'])
def test_volume_path_rejects_globs_traversal_and_wrong_parent_before_read(monkeypatch,path):
    monkeypatch.setattr(evidence,'_spark_bytes',lambda *a:pytest.fail('Invalid path must not reach Spark'))
    with pytest.raises(SilverError,match='RECEIPT_VOLUME_PATH_REQUIRED'):
        evidence.load_committed_evidence(path,PARENT,spark=object())


def test_volume_without_explicit_spark_reader_is_never_retried_via_fuse():
    with pytest.raises(SilverError,match='RECEIPT_SPARK_READER_REQUIRED'):
        evidence.load_committed_evidence(PATH,PARENT)

@pytest.mark.parametrize('payload,code',[(b'\xff','RECEIPT_INVALID_UTF8'),(b'null','RECEIPT_OBJECT_REQUIRED'),
    (b'{}','RECEIPT_PARENT_REQUIRED'),(b'{"parent_run_id":"other"}','REQUESTED_PARENT_MISMATCH'),
    (b'{"parent_run_id":1,"parent_run_id":2}','DUPLICATE_JSON_KEY')])
def test_volume_preserves_utf8_and_json_contracts(monkeypatch,payload,code):
    monkeypatch.setattr(evidence,'_spark_bytes',lambda *a:payload)
    with pytest.raises(SilverError,match='^'+code+'$'):evidence.load_committed_evidence(PATH,PARENT,spark=object())


def test_real_spark_binary_file_returns_all_utf8_bytes(spark,tmp_path):
    payload=json.dumps({'parent_run_id':PARENT,'padding':'á\n'*70000},ensure_ascii=False).encode()
    path=tmp_path/(PARENT+'.json');path.write_bytes(payload)
    assert evidence._spark_bytes(spark,str(path))==payload


def test_real_spark_missing_path_keeps_structured_error(spark,tmp_path):
    with pytest.raises(SilverError,match='RECEIPT_VOLUME_FILE_NOT_FOUND'):
        evidence._spark_bytes(spark,str(tmp_path/'missing.json'))


def test_real_spark_no_silent_truncation_or_multi_file_acceptance(spark,tmp_path,monkeypatch):
    (tmp_path/'a.json').write_bytes(b'{"a":1}')
    (tmp_path/'b.json').write_bytes(b'{"b":2}')
    with pytest.raises(SilverError,match='RECEIPT_SINGLE_FILE_REQUIRED'):
        evidence._spark_bytes(spark,str(tmp_path))
    monkeypatch.setattr(evidence,'MAX_RECEIPT_BYTES',3)
    with pytest.raises(SilverError,match='RECEIPT_SIZE_INVALID'):
        evidence._spark_bytes(spark,str(tmp_path/'a.json'))


@pytest.mark.parametrize('error_class,code',[('INSUFFICIENT_PERMISSIONS','RECEIPT_FILE_ACCESS_DENIED'),
    ('PERMISSION_DENIED','RECEIPT_FILE_ACCESS_DENIED'),('PATH_NOT_FOUND','RECEIPT_VOLUME_FILE_NOT_FOUND'),
    ('UNRECOGNIZED','RECEIPT_VOLUME_READ_FAILED'),(None,'RECEIPT_VOLUME_READ_FAILED')])
def test_spark_failure_never_falls_back_or_exposes_messages(error_class,code,monkeypatch):
    class RemoteError(Exception):
        def getErrorClass(self):return error_class
    class Reader:
        def format(self,value):assert value=='binaryFile';return self
        def load(self,path):raise RemoteError('synthetic-secret token private-path')
    monkeypatch.setattr(evidence,'open',lambda *a,**k:pytest.fail('No fallback'),raising=False)
    with pytest.raises(SilverError,match='^'+code+'$') as caught:
        evidence.load_committed_evidence(PATH,PARENT,spark=SimpleNamespace(read=Reader()))
    assert 'synthetic-secret' not in str(caught.value)


def test_volume_error_diagnostics_keep_only_allowlisted_provider_metadata(monkeypatch):
    from admm_silver.diagnostics import failure_report
    class AnalysisException(Exception):
        def getErrorClass(self):return 'CATALOG_NOT_FOUND'
    class Reader:
        def format(self,*a):return self
        def load(self,*a):raise AnalysisException('synthetic-secret authorization details')
    with pytest.raises(SilverError) as caught:
        evidence.load_committed_evidence(PATH,PARENT,spark=SimpleNamespace(read=Reader()))
    report=failure_report(caught.value,'committed_evidence')
    assert report['cause_type']=='AnalysisException'
    assert report['provider_error_class']=='CATALOG_NOT_FOUND'
    assert 'synthetic-secret' not in json.dumps(report)
