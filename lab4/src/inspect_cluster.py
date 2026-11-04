"""Inspect ZooKeeper directly over its API; never controls Docker or Git."""
import argparse
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from zk_protocol import ZKProtocol

DEFAULT_HOSTS = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'


def four_letter(host, port, command='srvr'):
    try:
        with socket.create_connection((host, port), timeout=2) as connection:
            connection.settimeout(2)
            connection.sendall(command.encode('ascii'))
            output = []
            while True:
                block = connection.recv(8192)
                if not block:
                    break
                output.append(block)
            return b''.join(output).decode('utf-8', errors='replace')
    except OSError as error:
        return f'{type(error).__name__}: {error}'


def server_states():
    return {str(port): four_letter('127.0.0.1', port) for port in (2181, 2182, 2183)}


def probe(hosts, expect):
    from kazoo.client import KazooClient
    client = KazooClient(hosts=hosts, timeout=10.0, read_only=False,
                         connection_retry={'max_tries': 1}, command_retry={'max_tries': 1})
    started = time.monotonic()
    result = {'expected': expect, 'successful': False}
    try:
        client.start(timeout=5)
        client.ensure_path('/de-lab4-ensemble')
        path = '/de-lab4-ensemble/probe'
        value = datetime.now(timezone.utc).isoformat().encode()
        if client.exists(path):
            client.set_async(path, value).get(timeout=5)
        else:
            client.create_async(path, value).get(timeout=5)
        actual, stat = client.get_async(path).get(timeout=5)
        result.update(successful=actual == value, value=actual.decode(), version=stat.version)
    except Exception as error:
        result.update(error=type(error).__name__, detail=str(error))
    finally:
        client.stop()
        client.close()
    result['elapsed_seconds'] = time.monotonic()-started
    result['expectation_met'] = result['successful'] == (expect == 'available')
    return result


def inspect_run(hosts, run_id):
    from zk_protocol import connect
    client = connect(hosts)
    try:
        protocol = ZKProtocol(client, run_id)
        values = {}
        for name in ('epoch', 'leader', 'checkpoint', 'result', 'settings'):
            path = protocol.base+'/'+name
            if not client.exists(path):
                values[name] = None
                continue
            value, stat = protocol.read(name)
            values[name] = {'value': value, 'version': stat.version,
                            'ephemeral_owner': hex(stat.ephemeralOwner)}
        return values
    finally:
        client.stop()
        client.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hosts', default=DEFAULT_HOSTS)
    p.add_argument('--expect', choices=['available', 'unavailable'], default='available')
    p.add_argument('--run-id')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    value = {'at_utc': datetime.now(timezone.utc).isoformat(), 'servers': server_states()}
    if args.run_id:
        value['run'] = inspect_run(args.hosts, args.run_id)
    else:
        value['client_probe'] = probe(args.hosts, args.expect)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(value, ensure_ascii=False, indent=2))
    if not args.run_id and not value['client_probe']['expectation_met']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
