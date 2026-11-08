"""Run the declared comparison against an explicitly isolated local database.

This reproducible audit never opens a configured production database, reads
credentials, constructs an exchange client or promotes a live policy.
"""

import argparse
import json
import sys
import time
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from StateStore import LendingStateStore  # noqa: E402
from StrategyResearch import evaluate_strategies, write_research_report  # noqa: E402
from StrategyV3 import StrategyPolicyV3, policy_v3_with_overrides  # noqa: E402
from V36Validation import validation_status  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--output', default='.state/research/strategy-validation-report.json')
    parser.add_argument('--principal', default='10000')
    args = parser.parse_args()
    path = Path(args.database).resolve()
    if path.name != 'isolated-v36.sqlite3' or not path.parent.name.startswith('validation-v36'):
        parser.error('only isolated-v36.sqlite3 inside validation-v36* may be audited')
    if not path.is_file():
        parser.error('isolated database does not exist')
    store = LendingStateStore(path)
    active = store.strategy('ACTIVE')
    if active is None:
        parser.error('the isolated copy must contain the actual baseline policy')
    policy = policy_v3_with_overrides(StrategyPolicyV3(), active['policy'])
    with store.read_connection() as connection:
        end = connection.execute('SELECT MAX(mts) FROM market_trades').fetchone()[0]
    if end is None:
        parser.error('the isolated copy contains no market prints')
    started = time.monotonic()

    def progress(item):
        print(json.dumps({'elapsedSeconds': round(time.monotonic() - started, 1), **item}, default=str), flush=True)

    report = evaluate_strategies(store, policy, Decimal(args.principal), now_ms=int(end) + 1, progress=progress)
    report['executionAudit'] = {
        'database': str(path), 'scope': 'isolated copy; no exchange calls or live activation',
        'elapsedSeconds': round(time.monotonic() - started, 3),
        'baselineVersionId': active['version_id'], 'certificateWritten': False,
    }
    write_research_report(args.output, report)
    candidate = policy_v3_with_overrides(StrategyPolicyV3(), report['selection']['candidatePolicy'])
    print(json.dumps({'phase': 'complete', 'reportPath': str(Path(args.output).resolve()),
                      'selection': report['selection'], 'liveCertificate': validation_status(store, candidate)},
                     ensure_ascii=False, default=str), flush=True)


if __name__ == '__main__':
    main()
