"""Avoid cmd.exe interpretation of REST URLs when Azure CLI is an MSI .cmd."""
import os
from pathlib import Path
from plan_guard import require


def native_argv(program,args,*,windows=None):
    windows=os.name=='nt' if windows is None else windows
    if windows and args[0]=='az' and Path(program).suffix.lower() in ('.cmd','.bat'):
        interpreter=Path(program).resolve().parent.parent/'python.exe'
        require(interpreter.is_file(),'AZURE_CLI_PACKAGED_PYTHON_NOT_FOUND')
        return [str(interpreter),'-X','utf8','-IBm','azure.cli',*map(str,args[1:])]
    return [program,*map(str,args[1:])]
