"""Prepare lab5, validate it locally, and run commands with persistent evidence."""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timezone

LAB = Path(__file__).resolve().parents[1]
REPO = LAB.parent
WORKSPACE = REPO.parents[1]
RUNTIME = LAB / '.runtime'
DATA = RUNTIME / 'objects'
sys.path.insert(0, str(LAB / 'src'))
from worker import pair_csv, read_pairs


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for data in iter(lambda: source.read(1048576), b''):
            h.update(data)
    return h.hexdigest()


def prepare():
    import csv
    source = WORKSPACE / 'lab3/generated/vacancies.jsonl'
    dataset = json.loads((REPO / 'lab3/manifests/dataset.json').read_text(encoding='utf-8'))
    if sha(source) != dataset['jsonl_sha256']:
        raise ValueError('Input differs from lab3')
    reference = WORKSPACE / 'lab3/artifacts/validation-20261101/results/mrall/counts.csv'
    counts = {}
    with reference.open(encoding='utf-8', newline='') as stream:
        for row in csv.DictReader(stream):
            if row['kind'] == '2':
                counts[tuple(json.loads(row['skills_json']))] = int(row['count'])
    if len(counts) != 15647 or sum(counts.values()) != 40778:
        raise ValueError('Reference differs from the lab3 report')
    folder = DATA / 'lab5/input'
    folder.mkdir(parents=True, exist_ok=True)
    reference_pairs = folder / 'reference.csv'
    reference_pairs.write_bytes(pair_csv(counts))
    # Keep the original object key, as in lab3, when migrating the snapshot.
    mirror = DATA / dataset['key']
    mirror.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, mirror)
    manifest = {'full': {'key': dataset['key'], 'sha256': dataset['jsonl_sha256'],
                         'records': dataset['file_count']},
                'reference': {'key': 'lab5/input/reference.csv', 'sha256': sha(reference_pairs)},
                'parts': []}
    with source.open('rb') as stream:
        for index in range(4):
            records = dataset['file_count'] // 4 + (index < dataset['file_count'] % 4)
            target = folder / f'part-{index}.jsonl'
            with target.open('wb') as output:
                for _ in range(records):
                    line = stream.readline()
                    if not line.strip():
                        raise ValueError('Unexpected empty or missing input record')
                    output.write(line)
            manifest['parts'].append({'index': index, 'key': f'lab5/input/part-{index}.jsonl',
                                      'records': records, 'sha256': sha(target)})
        if stream.read(1):
            raise ValueError('Unexpected trailing input')
    combined = hashlib.sha256()
    for part in manifest['parts']:
        combined.update((DATA / part['key']).read_bytes())
    if combined.hexdigest() != dataset['jsonl_sha256']:
        raise ValueError('Splitting changed the input')
    save(folder / 'manifest.json', manifest)
    save(RUNTIME / 'reference_top10.json', [
        {'skills': list(k), 'count': v}
        for k, v in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]])
    print('Prepared identical input, four complete parts, and all reference pairs.')


def capture(args, output, cwd=REPO, check=True, env=None):
    print('Running:', ' '.join(map(str, args)), flush=True)
    command_env = {**os.environ, 'PYTHONUTF8': '1', **(env or {})}
    result = subprocess.run(list(map(str, args)), cwd=cwd, capture_output=True,
                            encoding='utf-8', errors='strict', env=command_env)
    value = {'at_utc': datetime.now(timezone.utc).isoformat(), 'command': list(map(str, args)),
             'exit_code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}
    save(output, value)
    print(f'Exit {result.returncode}; transcript: {output}', flush=True)
    if result.returncode and check:
        raise RuntimeError(f'Command failed; see {output}')
    return value


def validate_local():
    prepare()
    run = LAB / 'evidence/local-validation'
    for index in ['full', '0', '1', '2', '3']:
        capture([sys.executable, '-B', LAB / 'src/worker.py', '--local-root', DATA,
                 '--index', index], run / f'map-{index}.json')
    capture([sys.executable, '-B', LAB / 'src/worker.py', '--local-root', DATA,
             '--mode', 'merge'], run / 'merge.json')
    # A Kubernetes retry must replace one index, without adding it twice.
    capture([sys.executable, '-B', LAB / 'src/worker.py', '--local-root', DATA,
             '--index', '0'], run / 'retry-part-0.json')
    capture([sys.executable, '-B', LAB / 'src/worker.py', '--local-root', DATA,
             '--mode', 'merge'], run / 'merge-after-retry.json')
    baseline = (DATA / 'lab5/input/reference.csv').read_bytes()
    for suffix in ['full', 'merged']:
        actual = (DATA / f'lab5/results/{suffix}/pairs.csv').read_bytes()
        if actual != baseline:
            raise ValueError(f'Output differs for {suffix}')
    partial = DATA / 'lab5/results/0/pairs.csv'
    original = partial.read_bytes()
    try:
        partial.write_bytes(original + b'\n')
        rejected = capture([sys.executable, '-B', LAB / 'src/worker.py', '--local-root', DATA,
                            '--mode', 'merge'], run / 'corrupted-part-rejected.json', check=False)
        if rejected['exit_code'] == 0 or 'Partial result checksum mismatch' not in rejected['stderr']:
            raise AssertionError('Corrupted result was not rejected for the expected reason')
    finally:
        partial.write_bytes(original)
    if (DATA / 'lab5/results/merged/pairs.csv').read_bytes() != baseline:
        raise AssertionError('Rejected merge overwrote the successful result')
    save(run / 'verification.json', {
        'execution': 'local Python validation; not a Kubernetes run',
        'records': 2057, 'unique_pairs': 15647, 'sum_pair_frequencies': 40778,
        'all_pairs_equal': True, 'reference_sha256': hashlib.sha256(baseline).hexdigest(),
        'retry_does_not_double_count': True, 'corrupted_part_rejected': True,
        'rejected_merge_keeps_previous_result': True,
        'top10': json.loads((RUNTIME / 'reference_top10.json').read_text(encoding='utf-8'))})
    print('Local validation passed: full and merged pair tables equal lab3 byte for byte.')


def free_memory():
    if os.name == 'nt':
        class MemoryStatus(ctypes.Structure):
            _fields_ = [('length', ctypes.c_ulong), ('load', ctypes.c_ulong),
                        *[(name, ctypes.c_ulonglong) for name in
                          ['total', 'available', 'total_page', 'available_page', 'total_virtual', 'available_virtual', 'extended']]]
        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise OSError('Cannot inspect free memory')
        return status.available
    return os.sysconf('SC_AVPHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')


def preflight():
    value = {'at_utc': datetime.now(timezone.utc).isoformat(),
             'available_ram_bytes': free_memory(), 'disk_free_bytes': shutil.disk_usage(WORKSPACE).free,
             'cluster_ram_mib': 2560, 'host_ram_reserve_mib': 2048,
             'disk_reserve_gib': 10}
    value['can_start'] = (value['available_ram_bytes'] >= (2560 + 2048) * 1048576
                          and value['disk_free_bytes'] >= 14 * 1073741824)
    save(RUNTIME / 'preflight.json', value)
    print(json.dumps(value, indent=2))
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'validate-local', 'preflight', 'cluster', 'build'])
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare()
    elif args.action == 'validate-local':
        validate_local()
    elif args.action == 'preflight':
        preflight()
    elif args.action == 'cluster':
        existing = subprocess.run(['docker','inspect','de-lab5','--format','{{.State.Running}}'],
                                  capture_output=True,text=True)
        if existing.returncode == 0 and existing.stdout.strip() == 'true':
            print('Existing de-lab5 node is already running; no extra cluster is started.')
            return
        if not preflight()['can_start']:
            raise RuntimeError('Not enough reserve: require 4.5 GiB free RAM and 14 GiB free disk before starting Docker')
        capture(['docker', 'desktop', 'start'], RUNTIME / 'docker-start.json')
        # MINIKUBE_HOME redirects profiles and downloaded cluster artifacts to D:.
        env = {**os.environ, 'MINIKUBE_HOME': str(RUNTIME / 'minikube')}
        capture([WORKSPACE / 'bin/minikube.exe', 'start', '-p', 'de-lab5', '--driver=docker',
                 '--cpus=3', '--memory=2560', '--disk-size=6g', '--container-runtime=containerd'],
                LAB / 'evidence/cluster/start.json', env=env)
        capture([WORKSPACE/'bin/minikube.exe','-p','de-lab5','kubectl','--',
                 '--context=de-lab5','get','nodes','-o','json'],
                LAB / 'evidence/cluster/nodes.json', env=env)
    elif args.action == 'build':
        env = {**os.environ, 'MINIKUBE_HOME': str(RUNTIME / 'minikube')}
        capture([WORKSPACE / 'bin/minikube.exe', '-p', 'de-lab5', 'image', 'build',
                 '-f', 'lab5/Dockerfile', '-t', 'de-lab5:local', '.'],
                LAB / 'evidence/cluster/build.json', env=env)


if __name__ == '__main__':
    main()
