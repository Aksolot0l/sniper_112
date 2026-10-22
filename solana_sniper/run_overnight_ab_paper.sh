#!/usr/bin/env bash
set -euo pipefail
set +x
PROJECT='/home/user/solana_sniper'
exec /usr/bin/env python3 "$PROJECT/run_overnight_ab_paper.py" "$@"
