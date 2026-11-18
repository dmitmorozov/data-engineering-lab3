"""Run the unchanged lab3 MapReduce and export its pair counts locally or to S3."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'lab3/src'))
from mapreduce import calculate
from common import byte_chunks, utc_now


def event(name, **fields):
    print(json.dumps({'at_utc': utc_now(), 'event': name, **fields}, ensure_ascii=False), flush=True)


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3', endpoint_url=os.environ['LAB5_S3_ENDPOINT'],
                        region_name='us-east-1',
                        aws_access_key_id=os.environ['LAB5_S3_ACCESS_KEY'],
                        aws_secret_access_key=os.environ['LAB5_S3_SECRET_KEY'],
                        config=Config(s3={'addressing_style': 'path'},
                                      retries={'max_attempts': 3, 'mode': 'standard'},
                                      connect_timeout=10, read_timeout=30))


def pair_csv(counts):
    buf = io.StringIO(newline='')
    writer = csv.writer(buf, lineterminator='\n')
    writer.writerow(['skill_a', 'skill_b', 'count'])
    for (a, b), count in sorted(counts.items()):
        writer.writerow([a, b, count])
    return buf.getvalue().encode('utf-8')


def read_pairs(payload):
    result = {}
    for row in csv.DictReader(io.StringIO(payload.decode('utf-8'))):
        key = (row['skill_a'], row['skill_b'])
        if key in result:
            raise ValueError('Duplicate pair in a result')
        result[key] = int(row['count'])
    return result


def memory_peak():
    """Cgroup-v2 peak includes mapper children; a host RSS value is not substituted."""
    cgroup = Path('/sys/fs/cgroup')
    for name in ('memory.peak', 'memory/memory.max_usage_in_bytes'):
        p = cgroup / name
        if p.exists():
            return {'source': str(p), 'bytes': int(p.read_text().strip())}
    return {'source': 'unavailable_outside_container', 'bytes': None}


def read_object(client, bucket, key):
    response = client.get_object(Bucket=bucket, Key=key)
    with response['Body'] as body:
        return body.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['map', 'merge'], default='map')
    parser.add_argument('--manifest', default='lab5/input/manifest.json')
    parser.add_argument('--prefix', default='lab5/results')
    parser.add_argument('--bucket', default='hh-lake')
    parser.add_argument('--index', default=os.getenv('JOB_COMPLETION_INDEX', 'full'))
    parser.add_argument('--local-root', type=Path)
    args = parser.parse_args()
    local = args.local_root
    client = None if local else s3_client()

    def get(key):
        return (local / key).read_bytes() if local else read_object(client, args.bucket, key)

    def put(key, payload):
        if local:
            p = local / key
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(payload)
        else:
            client.put_object(Bucket=args.bucket, Key=key, Body=payload)

    manifest = json.loads(get(args.manifest))
    started = time.perf_counter()
    event('started', mode=args.mode, index=args.index,
          backend='local_validation' if local else 's3', endpoint=None if local else os.getenv('LAB5_S3_ENDPOINT'))
    if args.mode == 'map':
        if args.index != 'full' and not 0 <= int(args.index) < len(manifest['parts']):
            raise ValueError('Completion index is outside the manifest')
        part = manifest['full'] if args.index == 'full' else manifest['parts'][int(args.index)]
        # Use a streaming body in the cluster; never download the whole input into RAM.
        if local:
            stream = (local / part['key']).open('rb')
        else:
            stream = client.get_object(Bucket=args.bucket, Key=part['key'])['Body']
        event('calculation_started', expected_records=part['records'])
        with stream:
            all_counts, stats = calculate(byte_chunks(stream, 262144), None,
                                          workers=2, batch_records=128, max_inflight=4,
                                          event=lambda value: event('mapreduce', **value))
        if stats['sha256'] != part['sha256'] or stats['records'] != part['records']:
            raise ValueError('Input hash or record count differs from manifest')
        counts = {k: v for k, v in all_counts.items() if len(k) == 2}
        del all_counts
        output = f'{args.prefix}/{args.index}'
        extra = {'input_sha256': stats['sha256'], 'records': stats['records'],
                 'committed_batches': stats['batches']}
    else:
        counts = Counter()
        records = 0
        for part in manifest['parts']:
            base = f"{args.prefix}/{part['index']}"
            summary = json.loads(get(base + '/summary.json'))
            payload = get(base + '/pairs.csv')
            if summary['input_sha256'] != part['sha256'] or summary['records'] != part['records']:
                raise ValueError('Partial result is from a different input')
            if hashlib.sha256(payload).hexdigest() != summary['pairs_sha256']:
                raise ValueError('Partial result checksum mismatch')
            counts.update(read_pairs(payload))
            records += summary['records']
        if records != manifest['full']['records']:
            raise ValueError('Missing or repeated input records')
        output = args.prefix + '/merged'
        extra = {'records': records, 'parts': len(manifest['parts'])}
    payload = pair_csv(counts)
    reference = get(manifest['reference']['key'])
    if hashlib.sha256(reference).hexdigest() != manifest['reference']['sha256']:
        raise ValueError('Reference checksum mismatch')
    if args.mode == 'merge' or args.index == 'full':
        expected = read_pairs(reference)
        if counts != expected:
            raise ValueError('Full pair table differs from lab3')
    top10 = [{'skills': list(k), 'count': v} for k, v in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]]
    put(output + '/pairs.csv', payload)
    summary = {'mode': args.mode, 'index': args.index, 'backend': 'local_validation' if local else 'kubernetes_s3',
               'at_utc': utc_now(), 'elapsed_seconds': time.perf_counter() - started,
               'unique_pairs': len(counts), 'pair_frequency_sum': sum(counts.values()),
               'pairs_sha256': hashlib.sha256(payload).hexdigest(), 'top10': top10,
               'memory_peak': memory_peak(), **extra}
    # Summary is the completion marker, written after the pair table. The reducer
    # reads each completion index once; a retried map replaces that index's result.
    put(output + '/summary.json', (json.dumps(summary, ensure_ascii=False, indent=2) + '\n').encode())
    event('completed', **summary)


if __name__ == '__main__':
    main()
