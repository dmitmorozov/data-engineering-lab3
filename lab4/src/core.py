"""Pure checkpoint/reduce logic. Lab 3 remains the source of counting rules."""
from __future__ import annotations
import hashlib
import json
import os
import re
import sys
import uuid
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'lab3/src'))
from mapreduce import map_batch, record_batches  # noqa: E402
from common import byte_chunks  # noqa: E402
from results import read_counts, write_counts  # noqa: E402


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(262144), b''):
            digest.update(block)
    return digest.hexdigest()


def validate_run_id(value):
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}', value):
        raise ValueError('run_id must contain 1..96 ASCII letters, digits, _ or -')
    return value


def empty_state(signature):
    return {'format': 1, 'signature': signature, 'next_batch': 0,
            'records': 0, 'batches': 0, 'emissions': [0, 0, 0], 'counts': Counter()}


def checkpoint_loaded_event(pointer, leader_token):
    """Distinguish the loading coordinator from the checkpoint's original author."""
    metadata = {key: value for key, value in pointer.items()
                if key not in ('coordinator_id', 'epoch', 'epoch_version')}
    return {'event': 'checkpoint_loaded', **metadata,
            'checkpoint_coordinator_id': pointer['coordinator_id'],
            'checkpoint_epoch': pointer['epoch'], 'epoch': leader_token['epoch']}


def apply_window(state, outputs):
    expected = state['next_batch']
    for result in outputs:
        if result['chunk_id'] != expected:
            raise ValueError('Window has a gap, duplicate, or wrong resume position')
        expected += 1
    # Validate the entire window before changing the accumulator.
    for result in outputs:
        for key, count in result['items']:
            state['counts'][tuple(key)] += count
        state['records'] += result['records']
        state['batches'] += 1
        state['emissions'] = [a+b for a, b in zip(state['emissions'], result['emissions'])]
    state['next_batch'] = expected


def snapshot_bytes(state):
    value = {k: v for k, v in state.items() if k != 'counts'}
    value['counts'] = [[list(key), count] for key, count in sorted(state['counts'].items())]
    return encoded(value)


def decode_snapshot(raw, expected_signature):
    value = json.loads(raw)
    if value.get('format') != 1 or value.get('signature') != expected_signature:
        raise ValueError('Checkpoint format/input/calculation parameters do not match')
    if value['next_batch'] != value['batches'] or value['records'] < 0:
        raise ValueError('Invalid checkpoint progress')
    counts = Counter()
    totals = [0, 0, 0]
    for key, count in value['counts']:
        key = tuple(key)
        if not 1 <= len(key) <= 3 or key in counts or not isinstance(count, int) or count <= 0:
            raise ValueError('Invalid or duplicate checkpoint key/count')
        counts[key] = count
        totals[len(key)-1] += count
    if totals != value['emissions']:
        raise ValueError('Checkpoint counters disagree with accumulated frequencies')
    value['counts'] = counts
    return value


class ObjectStore:
    """Shared durable directory; content-addressed snapshots, no pickle/network."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key):
        result = (self.root/key).resolve()
        if not result.is_relative_to(self.root):
            raise ValueError('Object key escapes store root')
        return result

    def put_snapshot(self, run_id, state):
        raw = snapshot_bytes(state)
        digest = hashlib.sha256(raw).hexdigest()
        key = f'{validate_run_id(run_id)}/objects/{digest}.json'
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix('.'+uuid.uuid4().hex+'.tmp')
        with temporary.open('xb') as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, target)
        return {'key': key, 'sha256': digest, 'bytes': len(raw),
                'next_batch': state['next_batch'], 'records': state['records']}

    def load_snapshot(self, pointer, signature):
        raw = self.path(pointer['key']).read_bytes()
        if hashlib.sha256(raw).hexdigest() != pointer['sha256']:
            raise ValueError('Checkpoint SHA-256 mismatch')
        state = decode_snapshot(raw, signature)
        if state['next_batch'] != pointer['next_batch'] or state['records'] != pointer['records']:
            raise ValueError('Checkpoint pointer and object progress disagree')
        return state


def make_signature(input_sha256, limit, batch_records, max_inflight):
    return {'input_sha256': input_sha256, 'limit': limit,
            'batch_records': batch_records, 'max_inflight': max_inflight,
            'semantics': 'lab3-first-unique-in-source-order-combinations-1-2-3'}
