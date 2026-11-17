#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$task_root"
mkdir -p scratch
# Go ignores nested modules, but does not obey .gitignore when walking ./... .
if [ ! -f scratch/go.mod ]; then
  printf 'module role-runtime-local-scratch\n\ngo 1.26.0\n' > scratch/go.mod
fi
if [ -n "${ROLE_RUNTIME_GO:-}" ]; then
  task_go=$ROLE_RUNTIME_GO
elif command -v go >/dev/null 2>&1; then
  task_go=$(command -v go)
elif [ -x scratch/toolchain/go/bin/go ]; then
  task_go="$task_root/scratch/toolchain/go/bin/go"
else
  printf '%s\n' 'Go 1.26.11 is required. Install Go or set ROLE_RUNTIME_GO to its binary.' >&2
  exit 1
fi
export GOCACHE="$task_root/scratch/go-cache"
export GOMODCACHE="$task_root/scratch/go-mod"
export GOTOOLCHAIN=local
exec "$task_go" "$@"
