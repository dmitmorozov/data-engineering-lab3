"""Verify actual attempts, checkpoints, epoch rejection and lab 3 frequencies."""
import argparse
import json
from pathlib import Path
from core import ObjectStore, read_counts, sha256_file


def read_events(path):
    if not Path(path).exists():
        return []
    raw = Path(path).read_text(encoding='utf-8')
    lines = raw.splitlines()
    if raw and not raw.endswith('\n'):
        lines = lines[:-1]  # A concurrently written last event is not complete yet.
    return [json.loads(line) for line in lines if line.strip()]


def verify(lab, run_id):
    evidence = lab/'evidence'/run_id
    experiment = json.loads((evidence/'experiment.json').read_text(encoding='utf-8'))
    final = json.loads((evidence/'final-pointer.json').read_text(encoding='utf-8'))
    before = json.loads((evidence/'backup-pointer.json').read_text(encoding='utf-8'))
    expected = read_counts(lab/'.runtime/input/reference.csv')
    store = ObjectStore(lab/'.runtime/state')
    events = {name: read_events(evidence/f'coordinator-{name}.jsonl') for name in ('A', 'B')}
    checks = {}
    comparisons = {}
    for name in ('A', 'B'):
        staged = [row for row in events[name] if row['event'] == 'result_staged']
        checks[f'{name}_one_staged_result'] = len(staged) == 1
        if staged:
            actual_path = store.path(staged[0]['key'])
            actual = read_counts(actual_path)
            differences = sum(expected.get(key) != actual.get(key) for key in expected.keys() | actual.keys())
            comparisons[name] = {'keys': len(actual), 'mismatched_keys': differences,
                                 'sha256': sha256_file(actual_path)}
            checks[f'{name}_matches_lab3'] = differences == 0
            checks[f'{name}_result_digest'] = comparisons[name]['sha256'] == staged[0]['sha256']
            checks[f'{name}_controls'] = (staged[0]['records'] == 2057 and
                staged[0]['emissions'] == [8810, 40778, 180490] and
                staged[0]['unique_keys'] == [1256, 15647, 84815])
    saved = [row for row in events['A'] if row['event'] == 'checkpoint_saved']
    loaded = [row for row in events['B'] if row['event'] == 'checkpoint_loaded']
    checks['backup_resumed_nonempty_checkpoint'] = (len(loaded) == 1 and
        0 < loaded[0]['records'] < 2057 and
        any(row['sha256'] == loaded[0]['sha256'] for row in saved))
    sessions = [row for values in events.values() for row in values if row['event'] == 'session_connected']
    checks['negotiated_session_timeout_10_seconds'] = bool(sessions) and all(
        row['negotiated_session_timeout_ms'] == 10000 for row in sessions)
    published = [row for values in events.values() for row in values if row['event'] == 'result_published']
    rejected = [row for values in events.values() for row in values if row['event'] == 'publication_rejected']
    checks['backup_published_before_resume'] = before['value'].get('coordinator_id') == 'B' and before['version'] == 1
    if experiment['mode'] == 'unsafe':
        checks['two_successful_publications'] = len(published) == 2 and {r['coordinator_id'] for r in published} == {'A', 'B'}
        checks['unsafe_final_pointer_A_version_2'] = final['value'].get('coordinator_id') == 'A' and final['version'] == 2
        checks['no_final_rejection_in_unsafe_mode'] = not rejected
    else:
        checks['one_successful_publication'] = len(published) == 1 and published[0]['coordinator_id'] == 'B'
        checks['stale_A_got_BadVersionError'] = len(rejected) == 1 and rejected[0]['coordinator_id'] == 'A' and rejected[0]['error'] == 'BadVersionError'
        checks['fenced_pointer_unchanged'] = final == before
    checks['failover_measured'] = 0 < experiment['failover_seconds'] < experiment['pause_seconds']
    checks['both_leader_epochs_visible'] = all(any(row['event'] == 'became_leader' for row in events[n]) for n in ('A', 'B'))
    official = read_counts(store.path(final['value']['key']))
    checks['official_result_matches_lab3'] = official == expected
    checks['official_hash_matches_pointer'] = sha256_file(store.path(final['value']['key'])) == final['value']['sha256']
    result = {'run_id': run_id, 'mode': experiment['mode'], 'passed': all(checks.values()),
              'checks': checks, 'comparisons': comparisons,
              'failover_seconds': experiment['failover_seconds'],
              'pause_seconds': experiment['pause_seconds'],
              'final_pointer': final}
    (evidence/'verification.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('run_id')
    args = p.parse_args()
    value = verify(Path(__file__).resolve().parents[1], args.run_id)
    print(json.dumps(value, ensure_ascii=False, indent=2))
    if not value['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
