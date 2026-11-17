#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$task_root"
task_unformatted=$(./scripts/go.sh fmt ./...)
if [ -n "$task_unformatted" ]; then
  printf 'Formatting was corrected; review changes and rerun:\n%s\n' "$task_unformatted" >&2
  exit 1
fi
./scripts/go.sh vet ./...
./scripts/go.sh test -race -count=1 ./...
if [ "${ROLE_RUNTIME_INTEGRATION:-0}" = "1" ]; then
  ./scripts/go.sh test -race -tags integration -count=1 ./internal/pispike
fi
python3 -B -m unittest discover -s scripts -p "test_*.py"
npm --prefix experiments/pi test
git diff --check
