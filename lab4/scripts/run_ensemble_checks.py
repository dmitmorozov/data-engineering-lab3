"""Check quorum with three, two and one ZooKeeper servers, then restore the ensemble."""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB/'src'))
from inspect_cluster import probe, server_states


def main():
    directory = LAB/'evidence/ensemble'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory.mkdir(parents=True, exist_ok=False)
    compose = ['docker', 'compose', '-f', str(LAB/'compose.yaml')]
    hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'

    def docker(*arguments):
        response = subprocess.run(compose+list(arguments), capture_output=True,
                                  encoding='utf-8', errors='replace', cwd=LAB)
        text = response.stdout+response.stderr
        if response.returncode:
            raise RuntimeError(text)
        return text

    def capture(name, expected, voters):
        states = server_states()
        result = {'at_utc': datetime.now(timezone.utc).isoformat(),
                  'servers': states, 'docker_ps': docker('ps', '-a'),
                  'client_probe': probe(hosts, expected)}
        leaders = sum('Mode: leader' in value for value in states.values())
        followers = sum('Mode: follower' in value for value in states.values())
        result['role_counts'] = {'leader': leaders, 'follower': followers}
        result['roles_match'] = (leaders == 1 and followers == voters-1) if voters > 1 else leaders == 0
        (directory/(name+'.json')).write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
        print(name+'\n'+json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if not result['client_probe']['expectation_met'] or not result['roles_match']:
            raise AssertionError(f'{name}: inspect evidence; expected {voters} working voters')

    stopped = False
    try:
        capture('01-three-online', 'available', 3)
        docker('stop', 'zoo3')
        stopped = True
        time.sleep(12)
        capture('02-two-online', 'available', 2)
        docker('stop', 'zoo2')
        time.sleep(12)
        capture('03-one-online-no-quorum', 'unavailable', 1)
    finally:
        if stopped:
            docker('start', 'zoo2', 'zoo3')
    time.sleep(12)
    capture('04-restored', 'available', 3)
    (directory/'server-output.txt').write_text(
        docker('logs', '--no-color', '--timestamps', 'zoo1', 'zoo2', 'zoo3'), encoding='utf-8')
    (directory/'versions.txt').write_text(
        docker('exec', '-T', 'zoo1', 'java', '-version')+'\n'+
        docker('images'), encoding='utf-8')
    print('ENSEMBLE_EVIDENCE='+str(directory), flush=True)


if __name__ == '__main__':
    main()
