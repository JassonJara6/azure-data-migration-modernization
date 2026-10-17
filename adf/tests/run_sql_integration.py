"""Create/dispose an isolated SQL Server Developer container for integration tests.

Requires Docker and 3 GB available RAM. Does not connect to an existing SQL
instance or Azure. The generated test password never appears in command args.
"""
import os
import secrets
import subprocess
import sys
import time
import tempfile
import importlib.util
from pathlib import Path


def main():
    name='codex-bronze-sql-test-'+secrets.token_hex(5)
    password='Test!;}{\\\'"$`&'+secrets.token_hex(24)
    env=dict(os.environ,MSSQL_SA_PASSWORD=password,SQLCMDPASSWORD=password)
    started=False
    temp=tempfile.TemporaryDirectory(prefix='bronze-odbc-')
    try:
        subprocess.run(['docker','run','-d','--name',name,'--memory','3g','-e','ACCEPT_EULA=Y',
           '-e','MSSQL_PID=Developer','-e','MSSQL_SA_PASSWORD','-p','127.0.0.1::1433','mcr.microsoft.com/mssql/server:2022-latest'],
           env=env,check=True,stdout=subprocess.DEVNULL)
        started=True
        ready=False;deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            result=subprocess.run(['docker','exec','-e','SQLCMDPASSWORD',name,'/opt/mssql-tools18/bin/sqlcmd',
              '-S','localhost','-U','sa','-C','-b','-Q','SELECT 1'],env=env,capture_output=True)
            if result.returncode==0:ready=True;break
            time.sleep(2)
        if not ready:raise RuntimeError('Isolated SQL Server did not become ready')
        if importlib.util.find_spec('pyodbc') is None:raise RuntimeError('Install pyodbc for the real ODBC integration tests')
        # Use the container's matching public ODBC driver in a temporary folder;
        # do not install or alter a system-wide driver/ODBC configuration.
        subprocess.run(['docker','cp',name+':/opt/microsoft/msodbcsql18',temp.name],check=True)
        drivers=list(Path(temp.name).glob('msodbcsql18/lib64/libmsodbcsql-*.so*'))
        if len(drivers)!=1:raise RuntimeError('ODBC test driver not found')
        port=subprocess.check_output(['docker','port',name,'1433/tcp'],text=True).strip().rsplit(':',1)[1]
        root=Path(__file__).resolve().parents[2]
        result=subprocess.run([sys.executable,'-m','pytest','-q','adf/tests/test_sqlserver_integration.py'],cwd=root,
          env=dict(os.environ,BRONZE_SQL_TEST_CONTAINER=name,BRONZE_SQL_TEST_PASSWORD=password,
                   BRONZE_SQL_TEST_DRIVER=str(drivers[0]),BRONZE_SQL_TEST_SERVER='127.0.0.1,'+port))
        return result.returncode
    finally:
        try:
            if started:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,check=True)
        finally:temp.cleanup()

if __name__=='__main__':raise SystemExit(main())
