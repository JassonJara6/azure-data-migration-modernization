"""Schema/CLI shape regressions; synthetic variants are not production evidence."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import launch
import diagnose
from compute_capacity import supports_driver_flexibility,select_alternates
from plan_guard import Blocked
from test_compute_capacity import inputs,ALTERNATES,PRIMARY


def test_schema_null_properties_and_boolean_subschemas():
    schema={'properties':None,'$defs':{'before':False,'cluster':{'properties':{'driver_node_type_flexibility':{}}}}}
    assert supports_driver_flexibility(schema)
    assert not supports_driver_flexibility({'properties':None,'items':[None,True]})


def test_numeric_string_quota_and_object_policy_normalized():
    a=inputs()
    for sku in a[2]:sku['family']='testFamily'
    a[3]['definition']=json.loads(a[3]['definition'])
    quotas=[{'name':{'value':n},'limit':'8','currentValue':'4'} for n in ('cores','testFamily')]
    assert select_alternates(*a,quotas)==ALTERNATES[PRIMARY]

@pytest.mark.parametrize('field,value,code',[('limit',None,'LIMIT'),('limit',True,'LIMIT'),('limit','NaN','LIMIT'),('limit',{},'LIMIT'),('currentValue',None,'CURRENT_VALUE'),('limit','4.5','CORES')])
def test_bad_quota_has_safe_field_code(field,value,code):
    a=inputs()
    for sku in a[2]:sku['family']='testFamily'
    quotas=[{'name':{'value':n},'limit':8,'currentValue':0} for n in ('cores','testFamily')]
    quotas[0][field]=value
    with pytest.raises(Blocked,match='INVALID_AZURE_QUOTA_'+code):select_alternates(*a,quotas)

@pytest.mark.parametrize('field,value,code',[('capabilities',None,'CAPABILITIES'),('locations',None,'LOCATIONS_OR_RESTRICTIONS'),('restrictions',None,'LOCATIONS_OR_RESTRICTIONS')])
def test_null_sku_collections_block_explicitly(field,value,code):
    a=inputs();a[2][1][field]=value
    with pytest.raises(Blocked,match='INVALID_AZURE_SKU_'+code):select_alternates(*a)


def test_null_memory_blocks_without_typeerror():
    a=inputs();a[2][1]['capabilities'][1]['value']=None
    with pytest.raises(Blocked,match='INVALID_AZURE_SKU_MEMORY'):select_alternates(*a)


def test_command_explicit_utf8_and_child_python_encoding(monkeypatch):
    def run(args,**kwargs):
        assert kwargs['encoding']=='utf-8' and kwargs['errors']=='strict'
        assert kwargs['env']['PYTHONUTF8']=='1' and kwargs['env']['PYTHONIOENCODING']=='utf-8'
        return NS(returncode=0,stdout='{"message":"validación ✓"}',stderr='')
    monkeypatch.setattr(launch.subprocess,'run',run)
    monkeypatch.setattr(launch.shutil,'which',lambda _:sys.executable)
    assert launch.command(['az','mock'],json_output=True)['message']=='validación ✓'
    assert diagnose.read_json(['az','mock'])[0]['message']=='validación ✓'


def test_invalid_bytes_never_echoed(monkeypatch):
    def run(*a,**k):raise UnicodeDecodeError('utf-8',b'synthetic-secret\xff',16,17,'bad')
    monkeypatch.setattr(launch.subprocess,'run',run)
    monkeypatch.setattr(launch.shutil,'which',lambda _:sys.executable)
    with pytest.raises(Blocked,match='CLI_OUTPUT_NOT_UTF8') as exc:launch.command(['az'])
    assert 'secret' not in str(exc.value)
    assert diagnose.read_json(['az'])[1]=={'code':'CLI_OUTPUT_NOT_UTF8'}


def test_unexpected_exception_reports_exact_code_line_no_values(monkeypatch,capsys):
    def failure():
        raise TypeError('synthetic-secret message')
    obj=NS(preflight_step='select_alternates',provision=failure)
    monkeypatch.setattr(launch,'Launcher',lambda *a,**k:obj)
    monkeypatch.setattr(launch,'load_config',lambda *a,**k:{})
    monkeypatch.setattr(sys,'argv',['launch.py','--stage','silver'])
    assert launch.main()==1
    output=capsys.readouterr().err
    data=json.loads(output)
    assert data['preflight_step']=='select_alternates'
    assert data['error_type']=='TypeError'
    assert data['locations'][-1]=={'file':'automation/silver/tests/test_preflight_diagnostics.py','line':failure.__code__.co_firstlineno+1}
    assert 'synthetic-secret' not in output and '/workspace' not in output


def test_real_child_utf8_json_with_non_ascii_output(monkeypatch):
    monkeypatch.setenv('PYTHONIOENCODING','cp1252')
    data=launch.command([sys.executable,'-c',"import json,sys; print(json.dumps({'text':'validación ✓','encoding':sys.stdout.encoding},ensure_ascii=False))"],json_output=True)
    assert data=={'text':'validación ✓','encoding':'utf-8'}
