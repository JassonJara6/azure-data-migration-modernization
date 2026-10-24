"""Real provider/CLI transport regression, entirely on loopback with synthetic OAuth.

Opt in after offline init of the workspace root: SILVER_PROVIDER_AUTH_TESTS=1.
No credential cache, Azure, Databricks service, backend or apply is used.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest

ROOT = Path(__file__).resolve().parents[3]
WORKSPACE = ROOT / 'terraform/databricks/workspace'
META = '00000000-0000-0000-0000-000000000002'
SYNTHETIC = 'synthetic-provider-regression-only'


@pytest.mark.skipif(os.environ.get('SILVER_PROVIDER_AUTH_TESTS') != '1', reason='Explicit local provider transport test opt-in required')
@pytest.mark.skipif(os.name == 'nt', reason='POSIX synthetic CLI fixture; real Windows secure-store acceptance remains operator validation')
@pytest.mark.parametrize('session_available', [True, False])
def test_real_provider_delegates_to_named_cli_session(tmp_path, session_available):
    terraform = os.environ.get('SILVER_TERRAFORM_BIN') or shutil.which('terraform')
    assert terraform, 'Terraform required for the explicitly enabled provider regression'
    assert (WORKSPACE / '.terraform/providers').is_dir(), 'Run offline workspace init first'
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.command, self.path))
            if self.path != '/api/2.1/unity-catalog/metastore_summary':
                self.send_error(404); return
            if self.headers.get('Authorization') != 'Bearer ' + SYNTHETIC:
                self.send_error(401); return
            body = json.dumps({'metastore_id': META, 'name': 'synthetic-only',
                               'owner': 'operator@example.invalid', 'region': 'eastus'}).encode()
            self.send_response(200); self.send_header('Content-Type', 'application/json')
            self.end_headers(); self.wfile.write(body)
        def do_POST(self):
            requests.append((self.command, self.path)); self.send_error(405)
        do_PUT = do_PATCH = do_DELETE = do_POST
        def log_message(self, *args): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try:
        log = tmp_path / 'cli-argv.jsonl'
        cli = tmp_path / 'native cli with spaces'
        # SDK distinguishes the modern Go CLI from legacy shims by a >1 MiB
        # executable size. This synthetic POSIX executable deliberately meets it.
        cli.write_text('#!' + sys.executable + '\n' + '''import json,os,sys
from pathlib import Path
with open(os.environ['FIXTURE_ARGV_LOG'],'a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[1:] != ['auth','token','--profile','secure-store-fixture']:
    sys.exit(20)
if os.environ['FIXTURE_SESSION_AVAILABLE'] != '1':
    print('synthetic selected session unavailable',file=sys.stderr);sys.exit(21)
print(json.dumps({'access_token':'synthetic-provider-regression-only','token_type':'Bearer','expiry':'2099-01-01T00:00:00Z'}))
''' + '\n#' + 'padding' * 160000 + '\n')
        cli.chmod(0o700)
        host = 'http://127.0.0.1:' + str(server.server_port)
        # All auth fields are explicit; no machine profile or token cache is read.
        (tmp_path / 'main.tf').write_text('''terraform {
  required_providers { databricks = { source = "databricks/databricks", version = "1.110.0" } }
}
provider "databricks" {
  host = ''' + json.dumps(host) + '''
  profile = "secure-store-fixture"
  auth_type = "databricks-cli"
}
data "databricks_current_metastore" "current" {}
output "metastore_id" { value = data.databricks_current_metastore.current.id }
''')
        shutil.copyfile(WORKSPACE / '.terraform.lock.hcl', tmp_path / '.terraform.lock.hcl')
        # CONFIG_FILE points at a synthetic host/profile only; it is a path
        # selector only. Never inspect or relocate the operator's auth cache.
        config = tmp_path / 'profile-config'
        config.write_text('[secure-store-fixture]\nhost = '+host+'\nauth_type = databricks-cli\n')
        env = {k:v for k,v in os.environ.items() if not k.startswith(('DATABRICKS_', 'TF_LOG', 'TF_VAR_'))}
        env.update(DATABRICKS_CLI_PATH=str(cli), DATABRICKS_CONFIG_FILE=str(config),
                   FIXTURE_ARGV_LOG=str(log), FIXTURE_SESSION_AVAILABLE='1' if session_available else '0')
        def run(*args):
            return subprocess.run([str(terraform), '-chdir=' + str(tmp_path), *args],
                                  env=env, text=True, capture_output=True, timeout=90)
        init = run('init', '-backend=false', '-input=false', '-lockfile=readonly',
                   '-plugin-dir=' + str(WORKSPACE / '.terraform/providers'))
        assert init.returncode == 0, 'Offline synthetic provider init failed'
        plan = run('plan', '-input=false', '-no-color', '-out=' + str(tmp_path / 'fixture.tfplan'))
        combined = plan.stdout + plan.stderr
        assert SYNTHETIC not in combined, 'Provider must not print even the synthetic bearer value'
        assert log.exists(), combined
        commands = [json.loads(line) for line in log.read_text().splitlines()]
        assert commands and all(c == ['auth','token','--profile','secure-store-fixture'] for c in commands)
        if session_available:
            assert plan.returncode == 0, combined
            assert requests and all(method == 'GET' for method, _ in requests)
            assert META in plan.stdout
        else:
            assert plan.returncode != 0 and 'synthetic selected session unavailable' in combined
            assert not requests, 'A failed selected session must not fall back to another identity'
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=5)
