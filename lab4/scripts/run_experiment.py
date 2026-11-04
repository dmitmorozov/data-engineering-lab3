"""Run two coordinators, pause the leader and verify failover against lab 3."""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB/'src'))
from core import encoded, validate_run_id
from zk_protocol import ZKProtocol, connect
from verify_run import read_events, verify


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['unsafe', 'fenced'], required=True)
    p.add_argument('--run-id', type=validate_run_id)
    p.add_argument('--pause-seconds', type=float, default=40)
    p.add_argument('--hosts', default='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
    args = p.parse_args()
    if args.pause_seconds < 35:
        p.error('Use approximately 40 seconds to demonstrate session expiration')
    run_id = args.run_id or args.mode+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    dest = LAB/'evidence'/run_id
    dest.mkdir(parents=True, exist_ok=False)
    environment = {**os.environ, 'LAB4_RUN_ID': run_id, 'LAB4_MODE': args.mode}
    compose = ['docker', 'compose', '-f', str(LAB/'compose.yaml'), '--profile', 'experiment']
    client = connect(args.hosts)
    protocol = ZKProtocol(client, run_id)
    if client.exists(protocol.base):
        raise ValueError('Run already exists in ZooKeeper; select a new run_id')
    protocol.initialize(args.mode)
    paused = False
    with (dest/'controller.jsonl').open('x', encoding='utf-8', buffering=1) as log:
        def record(event, **values):
            row = {'event': event, 'at_utc': datetime.now(timezone.utc).isoformat(),
                   'monotonic_ns': time.monotonic_ns(), **values}
            log.write(json.dumps(row, ensure_ascii=False)+'\n')
            print(json.dumps(row, ensure_ascii=False), flush=True)
            return row

        def docker(*arguments):
            command = compose+list(arguments)
            record('docker_command', arguments=command)
            response = subprocess.run(command, cwd=LAB, env=environment,
                                      capture_output=True, encoding='utf-8', errors='replace')
            if response.returncode:
                record('docker_command_failed', stdout=response.stdout, stderr=response.stderr)
                raise RuntimeError(response.stdout+response.stderr)
            return response.stdout

        def wait_for(label, callback, timeout=60):
            deadline, last_notice = time.monotonic()+timeout, 0
            while time.monotonic() < deadline:
                for name in ('A', 'B'):
                    fatals = [r for r in read_events(dest/f'coordinator-{name}.jsonl') if r['event'] == 'fatal_error']
                    if fatals:
                        raise RuntimeError(f'{name}: {fatals[-1]}')
                value = callback()
                if value:
                    return value
                if time.monotonic()-last_notice >= 5:
                    record('waiting', for_event=label)
                    last_notice = time.monotonic()
                time.sleep(.1)
            raise TimeoutError(label)

        def find_event(name, kind, predicate=lambda row: True):
            return next((row for row in read_events(dest/f'coordinator-{name}.jsonl')
                         if row['event'] == kind and predicate(row)), None)

        def pointer_file(name):
            value, stat = protocol.read('result')
            row = {'value': value, 'version': stat.version}
            (dest/name).write_bytes(encoded(row)+b'\n')
            record('result_pointer', file=name, **row)
            return row

        try:
            docker('up', '-d', '--no-build', '--pull', 'never', '--force-recreate', 'coord-a')
            wait_for('A became leader', lambda: find_event('A', 'became_leader'))
            docker('up', '-d', '--no-build', '--pull', 'never', '--force-recreate', 'coord-b')
            wait_for('B started waiting', lambda: find_event('B', 'waiting_for_leadership'))
            point = wait_for('A has a nonempty checkpoint and started another window', lambda:
                find_event('A', 'window_started', lambda row: row['committed_records'] > 0))
            # Bracket the OS command using one controller clock; timestamp after command
            # completion is the chosen pause origin, with command duration reported.
            pause_begin = time.monotonic()
            docker('pause', 'coord-a')
            pause_done = time.monotonic()
            paused = True
            record('pause_confirmed', committed_records=point['committed_records'],
                   command_seconds=pause_done-pause_begin)
            leader = wait_for('B became leader after expiration', lambda: find_event('B', 'became_leader'), 30)
            observed_leader = time.monotonic()
            failover = observed_leader-pause_done
            record('backup_leadership_observed', failover_seconds=failover,
                   observation_interval_seconds=.1, coordinator_event=leader)
            wait_for('B published', lambda: find_event('B', 'result_published'), 25)
            before = pointer_file('backup-pointer.json')
            if before['version'] != 1 or before['value'].get('coordinator_id') != 'B':
                raise AssertionError('Unexpected pointer before old leader resume')
            remaining = args.pause_seconds-(time.monotonic()-pause_done)
            if remaining < 0:
                raise RuntimeError('Backup did not finish within chosen pause duration; tune delay and rerun')
            while time.monotonic()-pause_done < args.pause_seconds:
                time.sleep(max(0, min(1, args.pause_seconds-(time.monotonic()-pause_done))))
            docker('unpause', 'coord-a')
            pause_actual = time.monotonic()-pause_done
            paused = False
            record('resume_confirmed', pause_seconds=pause_actual)
            terminal = 'result_published' if args.mode == 'unsafe' else 'publication_rejected'
            wait_for('A final publication outcome', lambda: find_event('A', terminal), 70)
            pointer_file('final-pointer.json')
            experiment = {'run_id': run_id, 'mode': args.mode, 'failover_seconds': failover,
                          'pause_seconds': pause_actual, 'pause_command_seconds': pause_done-pause_begin,
                          'observation_interval_seconds': .1,
                          'pause_origin': 'controller monotonic time immediately after Docker pause returned',
                          'leader_observation': 'controller observed flushed B became_leader event',
                          'demonstration_delay_per_window_seconds': 3}
            (dest/'experiment.json').write_bytes(encoded(experiment)+b'\n')
            outcome = verify(LAB, run_id)
            record('verification', passed=outcome['passed'], checks=outcome['checks'])
            print(f'RUN_ID={run_id}\nEVIDENCE={dest}', flush=True)
            if not outcome['passed']:
                raise AssertionError('Evidence verification failed')
        finally:
            if paused:
                docker('unpause', 'coord-a')
            client.stop()
            client.close()


if __name__ == '__main__':
    main()
