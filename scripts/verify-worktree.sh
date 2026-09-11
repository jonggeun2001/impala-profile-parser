#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -x .venv/bin/python ]]; then
  parser_python=.venv/bin/python
else
  parser_python=python3.9
fi
"$parser_python" -c 'import sys; assert sys.version_info[:2] == (3, 9), "verification requires Python 3.9"'
"$parser_python" -m ruff check src tests scripts
"$parser_python" -m pytest -q
