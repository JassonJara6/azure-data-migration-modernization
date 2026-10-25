#!/usr/bin/env bash
# One read-only collection; no deployment, Terraform, SQL, cluster or Job execution.
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8
cd "$(dirname "$0")/../.."
exec python automation/silver/collect_workspace.py "$@"
