#!/bin/sh
# Local CI: the same gates the software factory runs.
set -eu
cd "$(dirname "$0")/.."
make ci
