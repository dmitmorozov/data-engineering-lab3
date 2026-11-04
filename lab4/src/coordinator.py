"""A real resumable lab-3 coordinator with one intentionally unsafe publication mode."""
from __future__ import annotations
import argparse
import concurrent.futures
import itertools
import json
import multiprocessing
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from core import (ROOT, ObjectStore, apply_window, byte_chunks, checkpoint_loaded_event, empty_state, encoded,
                  make_signature, map_batch, record_batches, sha256_file, validate_run_id,
                  write_counts)
from zk_protocol import ZKProtocol, connect


def run(args, event):
    from kazoo.exceptions import BadVersionError, ConnectionLoss, SessionExpiredError
    manifest = json.loads((ROOT/'lab3/manifests/dataset.json').read_text(encoding='utf-8'))
    digest = sha256_file(args.input)
    if digest != manifest['jsonl_sha256']:
        raise ValueError('Input differs from lab 3 manifest')
    signature = make_signature(digest, args.limit, args.batch_records, args.max_inflight)
    store = ObjectStore(args.store)
    zk = connect(args.hosts, event)
    try:
        protocol = ZKProtocol(zk, args.run_id)
        protocol.initialize(args.mode)
        event(event='waiting_for_leadership', input_sha256=digest, signature=signature)
        while True:
            if protocol.read('result')[0]:
                event(event='finished_by_other_coordinator')
                return
            token = protocol.claim(args.coordinator_id)
            if token:
                break
            time.sleep(.1)
        event(event='became_leader', **token)
        pointer, _ = protocol.read('checkpoint')
        state = store.load_snapshot(pointer, signature) if pointer else empty_state(signature)
        if pointer:
            event(**checkpoint_loaded_event(pointer, token))
        stale = False
        input_stats = {'read_bytes': 0, 'network_chunks': 0}
        resumed_batch = state['next_batch']
        # Same mapper function and two worker processes as lab 3. Checkpoint per full window.
        with args.input.open('rb') as source, concurrent.futures.ProcessPoolExecutor(
                max_workers=args.workers, mp_context=multiprocessing.get_context('spawn')) as pool:
            batches = enumerate(record_batches(byte_chunks(source, 262144),
                                               args.batch_records, input_stats))
            pending = ((number, rows) for number, rows in batches if number >= resumed_batch)
            while True:
                window = list(itertools.islice(pending, args.max_inflight))
                if not window:
                    break
                event(event='window_started', first_batch=window[0][0],
                      committed_records=state['records'], demonstration_delay_seconds=args.delay)
                time.sleep(args.delay)
                jobs = [(number, rows, args.limit, True, False) for number, rows in window]
                outputs = list(pool.map(map_batch, jobs))
                apply_window(state, outputs)
                event(event='window_reduced', next_batch=state['next_batch'],
                      records=state['records'])
                if not stale:
                    snapshot = store.put_snapshot(args.run_id, state)
                    checkpoint = {**snapshot, **token}
                    try:
                        protocol.fenced_set('checkpoint', checkpoint, token)
                        event(event='checkpoint_saved', **checkpoint)
                    except (BadVersionError, ConnectionLoss, SessionExpiredError) as error:
                        # This experiment lets an old attempt finish locally so its final
                        # publication can exercise the actual fencing gate. Shared checkpoint
                        # updates remain fenced in BOTH modes; no further updates after this.
                        stale = True
                        event(event='checkpoint_write_rejected', error=type(error).__name__,
                              old_epoch=token['epoch'], demonstration_stale_continuation=True)
        if input_stats.get('sha256') != digest or state['records'] != manifest['file_count']:
            raise ValueError('Full input validation or final record count failed')
        destination = store.path(f'{args.run_id}/attempts/{args.coordinator_id}-epoch-{token["epoch"]}')
        dimensions = write_counts(destination, state['counts'].items())
        result = {**token, 'key': str((destination/'counts.csv').relative_to(store.root)).replace('\\','/'),
                  'sha256': sha256_file(destination/'counts.csv'), 'records': state['records'],
                  'emissions': state['emissions'], 'resumed_from_batch': resumed_batch,
                  'input_sha256': digest, **dimensions}
        (destination/'summary.json').write_bytes(encoded(result)+b'\n')
        event(event='result_staged', **result)
        # A fresh connection permits the expired attempt to reach the storage-side fence.
        # Its original token is retained; this connection DOES NOT re-acquire leadership.
        writer = connect(args.hosts, event)
        try:
            publication = ZKProtocol(writer, args.run_id)
            try:
                stat = publication.publish(result, token, args.mode)
            except BadVersionError:
                current_epoch, _ = publication.read('epoch')
                current_result, stat = publication.read('result')
                event(event='publication_rejected', error='BadVersionError',
                      old_epoch=token['epoch'], current_epoch=current_epoch['epoch'],
                      result_version=stat.version,
                      result_coordinator=current_result.get('coordinator_id'))
            else:
                event(event='result_published', message='записал результат',
                      result_version=stat.version, **result)
        finally:
            writer.stop()
            writer.close()
    finally:
        zk.stop()
        zk.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hosts', default=os.getenv('LAB4_ZK_HOSTS', '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'))
    p.add_argument('--coordinator-id', choices=['A', 'B'], default=os.getenv('LAB4_COORDINATOR_ID', 'A'))
    p.add_argument('--run-id', required=True, type=validate_run_id)
    p.add_argument('--mode', choices=['unsafe', 'fenced'], required=True)
    p.add_argument('--input', required=True, type=Path)
    p.add_argument('--store', required=True, type=Path)
    p.add_argument('--evidence', required=True, type=Path)
    p.add_argument('--limit', choices=['5', '10', 'all'], default='all')
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--batch-records', type=int, default=128)
    p.add_argument('--max-inflight', type=int, default=4)
    p.add_argument('--delay', type=float, default=3)
    args = p.parse_args()
    if min(args.workers, args.batch_records, args.max_inflight) < 1 or args.delay < 0:
        p.error('Worker/window sizes must be positive; delay must be nonnegative')
    args.limit = None if args.limit == 'all' else int(args.limit)
    target = args.evidence/args.run_id
    target.mkdir(parents=True, exist_ok=True)
    # Refuse log overwrites: every independent experiment gets its own run_id.
    with (target/f'coordinator-{args.coordinator_id}.jsonl').open('x', encoding='utf-8', buffering=1) as log:
        def event(**value):
            row = {'at_utc': datetime.now(timezone.utc).isoformat(),
                   'monotonic_ns': time.monotonic_ns(), 'run_id': args.run_id,
                   'coordinator_id': args.coordinator_id, 'pid': os.getpid(), **value}
            line = json.dumps(row, ensure_ascii=False)
            log.write(line+'\n')
            log.flush()
            print(line, flush=True)
        try:
            run(args, event)
        except Exception as error:
            event(event='fatal_error', error=type(error).__name__, detail=str(error))
            traceback.print_exc()
            raise


if __name__ == '__main__':
    main()
