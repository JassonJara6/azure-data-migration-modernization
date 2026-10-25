"""Fresh wheel builds without deleting a synchronized staging dist directory."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile
import zipfile
from email.parser import Parser
from plan_guard import require, sha


def source_version(project):
    inputs=[project/'pyproject.toml',*sorted(p for p in (project/'src').rglob('*')
        if p.is_file() and not any(part=='__pycache__' or part.endswith('.egg-info') for part in p.parts))]
    digest=hashlib.sha256(b''.join(p.relative_to(project).as_posix().encode()+b'\0'+p.read_bytes()+b'\0' for p in inputs)).hexdigest()[:12]
    return '0.1.0+'+digest


def wheel_version(wheel):
    try:
        with zipfile.ZipFile(wheel) as archive:
            entries=[name for name in archive.namelist() if name.endswith('.dist-info/METADATA')]
            require(len(entries)==1,'INVALID_SILVER_WHEEL_METADATA')
            metadata=Parser().parsestr(archive.read(entries[0]).decode('utf-8'))
        names=metadata.get_all('Name',[]);versions=metadata.get_all('Version',[])
        require(len(names)==1 and names[0].lower().replace('_','-')=='admm-silver' and len(versions)==1,'INVALID_SILVER_WHEEL_METADATA')
        version=versions[0]
        require(re.fullmatch(r'0\.1\.0\+[0-9a-f]{12}',version) and wheel.name.startswith('admm_silver-'+version+'-'),'INVALID_SILVER_WHEEL_VERSION')
        return version
    except (zipfile.BadZipFile,UnicodeError,KeyError,OSError):
        require(False,'INVALID_SILVER_WHEEL_METADATA')


def build_wheel(source, stage, epoch, env, run):
    require(str(epoch).isdigit(), 'REPRODUCIBLE_BUILD_EPOCH_REQUIRED')
    # A new build tree avoids both stale setuptools build/lib and OneDrive locks.
    scratch = Path(tempfile.mkdtemp(prefix='admm-silver-build-'))
    try:
        project = scratch/'project'; output = scratch/'output'; output.mkdir()
        shutil.copytree(source, project, ignore=shutil.ignore_patterns(
            '.databricks','build','dist','*.egg-info','__pycache__'))
        source_hash=source_version(project).split('+',1)[1]
        config = project/'pyproject.toml'
        changed, count = re.subn(r'(?m)^version = .*$', f'version = "0.1.0+{source_hash}"', config.read_text())
        require(count == 1, 'EXACTLY_ONE_PROJECT_VERSION_REQUIRED'); config.write_text(changed)
        run([sys.executable,'-m','build','--wheel','--outdir',str(output)],cwd=project,
            env=dict(env,SOURCE_DATE_EPOCH=str(epoch)))
        wheels = list(output.glob('*.whl'))
        require(len(wheels) == 1 and wheels[0].is_file(), 'EXACTLY_ONE_FRESH_WHEEL_REQUIRED')
        # Never unlink, rename, overwrite, or rmtree the synchronized dist tree.
        dist = stage/'dist'; dist.mkdir(parents=True,exist_ok=True)
        selected = Path(tempfile.mkdtemp(prefix='wheel-',dir=dist))/wheels[0].name
        expected = sha(wheels[0])
        with wheels[0].open('rb') as original, selected.open('xb') as destination:
            shutil.copyfileobj(original,destination)
        require(sha(selected) == expected, 'WHEEL_COPY_SHA256_MISMATCH')
        return selected
    finally:
        # Cleanup is optional and never targets stage/dist. AV/Windows locks on
        # a temporary tree must not invalidate an already copied/verified wheel.
        try: shutil.rmtree(scratch)
        except OSError: print(json.dumps({'status':'BUILD_TEMP_CLEANUP_DEFERRED'}))


def pin_wheel(spec, stage, wheel):
    relative = './'+wheel.relative_to(stage).as_posix()
    artifact = spec['artifacts']['silver_wheel']
    artifact.update(build='python verify_artifact.py',dynamic_version=False,files=[{'source':relative}])
    task = spec['resources']['jobs']['silver']['tasks'][0]
    require(len(task.get('libraries',[])) == 1 and 'whl' in task['libraries'][0], 'SINGLE_WHEEL_LIBRARY_REQUIRED')
    task['libraries'][0]['whl'] = relative
    verifier = ("from pathlib import Path\nimport hashlib\np=Path("+repr(wheel.relative_to(stage).as_posix())+")\n"
                "if hashlib.sha256(p.read_bytes()).hexdigest() != "+repr(sha(wheel))+":\n"
                "    raise SystemExit('APPROVED_WHEEL_SHA256_MISMATCH')\n")
    (stage/'verify_artifact.py').write_text(verifier,encoding='utf-8')


def selected_wheel(stage, spec):
    files = spec.get('artifacts',{}).get('silver_wheel',{}).get('files',[])
    require(len(files) == 1, 'EXACTLY_ONE_SELECTED_WHEEL_REQUIRED')
    source = files[0].get('source','')
    require(isinstance(source,str) and source and not any(c in source for c in '*?[]'), 'EXACT_WHEEL_PATH_REQUIRED')
    path = (stage/source).resolve()
    require(path.is_relative_to((stage/'dist').resolve()) and path.suffix == '.whl' and path.is_file(),
            'SELECTED_WHEEL_MISSING_OR_OUTSIDE_DIST')
    tasks = spec.get('resources',{}).get('jobs',{}).get('silver',{}).get('tasks',[])
    require(len(tasks) == 1 and tasks[0].get('libraries') == [{'whl':source}], 'WHEEL_ARTIFACT_LIBRARY_MISMATCH')
    return path
