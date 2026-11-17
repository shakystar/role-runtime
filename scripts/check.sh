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
npm --prefix experiments/pi test
git diff --check
