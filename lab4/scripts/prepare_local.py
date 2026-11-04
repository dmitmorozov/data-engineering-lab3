"""Prepare input and reference tables from an existing lab 3 workspace."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, required=True)
    args = parser.parse_args()
    lab = Path(__file__).resolve().parents[1]
    manifest = json.loads((lab.parent/'lab3/manifests/dataset.json').read_text(encoding='utf-8'))
    source = args.workspace/'lab3/generated/vacancies.jsonl'
    reference = args.workspace/'lab3/artifacts/validation-20261101/results/mrall/counts.csv'
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    if digest != manifest['jsonl_sha256']:
        raise ValueError('Local input does not match lab 3 manifest')
    target = lab/'.runtime/input'
    target.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target/'vacancies.jsonl')
    shutil.copyfile(reference, target/'reference.csv')
    (lab/'.runtime/state').mkdir(parents=True, exist_ok=True)
    evidence = lab/'evidence/environment'
    evidence.mkdir(parents=True, exist_ok=True)
    value = {'input_sha256': digest, 'input_bytes': source.stat().st_size,
             'input_records': sum(bool(line.strip()) for line in source.open('rb')),
             'reference_sha256': hashlib.sha256(reference.read_bytes()).hexdigest(),
             'reference_source': 'lab3/artifacts/validation-20261101/results/mrall/counts.csv',
             'input_origin': manifest['uri'], 'execution_input': 'identical local mirror',
             'checkpoint_storage': 'shared durable filesystem volume'}
    (evidence/'baseline.json').write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(value, indent=2))


if __name__ == '__main__':
    main()
