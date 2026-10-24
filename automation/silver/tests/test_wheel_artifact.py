"""Local wheel preparation tests. No Databricks/Azure/SQL calls."""
import os
from pathlib import Path
import subprocess
import sys
import zipfile
import pytest
import yaml
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import launch
from plan_guard import Blocked,sha
import wheel_artifact as artifact
ROOT=Path(__file__).resolve().parents[3]


def source_tree(tmp_path):
    source=tmp_path/'source';(source/'src/package').mkdir(parents=True)
    (source/'src/package/current.py').write_text('VALUE=1\n')
    (source/'pyproject.toml').write_text('[project]\nname="synthetic"\nversion = "0.1.0"\n')
    return source


def synthetic_build(args,**kwargs):
    assert args[1:4]==['-m','build','--wheel'] and '--outdir' in args
    assert kwargs['env']['SOURCE_DATE_EPOCH']=='1700000000'
    project=Path(kwargs['cwd'])
    assert not (project/'build').exists() and not (project/'src/package/obsolete.py').exists()
    out=Path(args[args.index('--outdir')+1]);assert not list(out.iterdir())
    (out/'synthetic-0.1.0-py3-none-any.whl').write_bytes(b'fresh reproducible synthetic wheel')


def spec():
    return {'artifacts':{'silver_wheel':{'type':'whl','path':'.'}},
            'resources':{'jobs':{'silver':{'tasks':[{'libraries':[{'whl':'./dist/*.whl'}]}]}}}}


def test_repeated_builds_survive_windows_rmtree_denied_without_touching_dist(tmp_path,monkeypatch):
    source=source_tree(tmp_path);stage=tmp_path/'bundle';(stage/'dist').mkdir(parents=True)
    (stage/'dist/old.whl').write_bytes(b'old');(stage/'src/package').mkdir(parents=True)
    (stage/'src/package/obsolete.py').write_text('removed from current source')
    (source/'build/lib/package').mkdir(parents=True);(source/'build/lib/package/obsolete.py').write_text('stale build cache')
    attempted=[]
    def denied(path,*args,**kwargs):
        attempted.append(Path(path));raise PermissionError('[WinError 5] Acceso denegado')
    monkeypatch.setattr(artifact.shutil,'rmtree',denied)
    first=artifact.build_wheel(source,stage,'1700000000',{},synthetic_build)
    second=artifact.build_wheel(source,stage,'1700000000',{},synthetic_build)
    assert first!=second and first.name==second.name and sha(first)==sha(second)
    assert (stage/'dist/old.whl').read_bytes()==b'old'
    assert attempted and all(not p.is_relative_to(stage) for p in attempted)


@pytest.mark.parametrize('number',[0,2])
def test_no_fallback_to_old_wheel_when_build_output_is_missing_or_ambiguous(tmp_path,number):
    source=source_tree(tmp_path);stage=tmp_path/'bundle';(stage/'dist').mkdir(parents=True)
    (stage/'dist/old.whl').write_bytes(b'old')
    def build(args,**kwargs):
        out=Path(args[args.index('--outdir')+1])
        for n in range(number):(out/(str(n)+'.whl')).write_bytes(b'synthetic')
    with pytest.raises(Blocked,match='ONE_FRESH_WHEEL'):artifact.build_wheel(source,stage,'1700000000',{},build)
    assert list((stage/'dist').iterdir())==[stage/'dist/old.whl']


def test_build_failure_does_not_reuse_an_old_artifact(tmp_path):
    source=source_tree(tmp_path);stage=tmp_path/'bundle';(stage/'dist').mkdir(parents=True)
    (stage/'dist/old.whl').write_bytes(b'old')
    def failure(*args,**kwargs):raise Blocked('SYNTHETIC_BUILD_FAILURE')
    with pytest.raises(Blocked,match='BUILD_FAILURE'):artifact.build_wheel(source,stage,'1700000000',{},failure)


def test_exact_selection_and_sha_verifier_ignore_unused_wheels_but_detect_tampering(tmp_path):
    stage=tmp_path/'bundle';(stage/'dist/fresh').mkdir(parents=True)
    wheel=stage/'dist/fresh/current.whl';wheel.write_bytes(b'current synthetic wheel')
    stale=stage/'dist/old.whl';stale.write_bytes(b'old')
    config=spec();artifact.pin_wheel(config,stage,wheel)
    (stage/'databricks.yml').write_text(yaml.safe_dump(config))
    (stage/'notebooks').mkdir();(stage/'notebooks/run.py').write_text('# current notebook')
    original=launch.deployment_sha(stage)
    assert artifact.selected_wheel(stage,config)==wheel.resolve()
    stale.write_bytes(b'old changed');assert launch.deployment_sha(stage)==original
    def verify():return subprocess.run([sys.executable,'-O','verify_artifact.py'],cwd=stage,capture_output=True,text=True)
    assert verify().returncode==0
    wheel.write_bytes(b'tampered');result=verify()
    assert result.returncode!=0 and 'APPROVED_WHEEL_SHA256_MISMATCH' in result.stderr
    assert launch.deployment_sha(stage)!=original


@pytest.mark.parametrize('bad',['./dist/*.whl','../outside.whl','./dist/absent.whl'])
def test_no_wildcard_outside_or_absent_selected_artifact(tmp_path,bad):
    config=spec();config['artifacts']['silver_wheel']['files']=[{'source':bad}]
    with pytest.raises(Blocked):artifact.selected_wheel(tmp_path,config)


def test_artifact_and_task_cannot_select_different_wheels(tmp_path):
    wheel=tmp_path/'dist/current.whl';wheel.parent.mkdir();wheel.write_bytes(b'current')
    config=spec();artifact.pin_wheel(config,tmp_path,wheel)
    config['resources']['jobs']['silver']['tasks'][0]['libraries']=[{'whl':'./dist/old.whl'}]
    with pytest.raises(Blocked,match='LIBRARY_MISMATCH'):artifact.selected_wheel(tmp_path,config)


@pytest.mark.skipif(os.environ.get('SILVER_WHEEL_BUILD_TESTS')!='1',reason='Opt in to local real wheel builds')
def test_real_wheel_is_reproducible_and_contains_only_current_package(tmp_path):
    stage=tmp_path/'bundle';(stage/'dist').mkdir(parents=True)
    (stage/'dist/stale.whl').write_bytes(b'not a wheel')
    (stage/'src/admm_silver').mkdir(parents=True)
    (stage/'src/admm_silver/obsolete.py').write_text('must not enter the wheel')
    def run(args,**kwargs):
        result=subprocess.run(args,capture_output=True,text=True,timeout=180,**kwargs)
        assert result.returncode==0,result.stderr
    env=dict(os.environ)
    first=artifact.build_wheel(ROOT/'databricks/silver',stage,'1700000000',env,run)
    second=artifact.build_wheel(ROOT/'databricks/silver',stage,'1700000000',env,run)
    assert first!=second and first.name==second.name and sha(first)==sha(second)
    with zipfile.ZipFile(second) as wheel:
        assert 'admm_silver/contract.json' in wheel.namelist()
        assert not any('obsolete' in name for name in wheel.namelist())
