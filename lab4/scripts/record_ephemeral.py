"""Record an ephemeral znode and its persistent parent."""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB/'src'))
from zk_protocol import connect


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--phase', choices=['exists', 'gone'], required=True)
    p.add_argument('--path', required=True)
    args = p.parse_args()
    if not args.path.startswith('/lab4-ephemeral-demo/') or args.path.count('/') != 2:
        p.error('Use /lab4-ephemeral-demo/<unique-name>')
    client = connect('127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
    try:
        stat = client.exists(args.path)
        parent = client.exists('/lab4-ephemeral-demo')
        value = {'at_utc': datetime.now(timezone.utc).isoformat(), 'phase': args.phase,
                 'path': args.path, 'child_exists': stat is not None,
                 'ephemeral_owner': hex(stat.ephemeralOwner) if stat else None,
                 'parent_exists': parent is not None}
        expected = bool(stat and stat.ephemeralOwner) if args.phase == 'exists' else stat is None and parent is not None
        value['passed'] = expected
        destination = LAB/'evidence/ephemeral'/args.path.rsplit('/', 1)[1]
        destination.mkdir(parents=True, exist_ok=True)
        target = destination/(args.phase+'.json')
        if target.exists():
            raise ValueError('Observation already exists; use a new unique child name')
        target.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
        print(json.dumps(value, indent=2))
        if not expected:
            raise SystemExit(1)
    finally:
        client.stop()
        client.close()


if __name__ == '__main__':
    main()
