"""Small ZooKeeper pointers; atomic epoch fencing applies at the storage boundary."""
import json
import time
from core import encoded, validate_run_id


class ZKProtocol:
    def __init__(self, client, run_id):
        self.zk = client
        self.base = '/de-lab4/'+validate_run_id(run_id)

    def initialize(self, mode):
        from kazoo.exceptions import NodeExistsError
        self.zk.ensure_path(self.base)
        for name, value in [('epoch', {'epoch': 0}), ('checkpoint', {}),
                            ('result', {}), ('settings', {'mode': mode})]:
            try:
                self.zk.create(self.base+'/'+name, encoded(value))
            except NodeExistsError:
                pass
        if self.read('settings')[0] != {'mode': mode}:
            raise ValueError('Run namespace already belongs to a different experiment mode')

    def read(self, name):
        raw, stat = self.zk.get(self.base+'/'+name)
        return json.loads(raw), stat

    def claim(self, coordinator_id):
        from kazoo.exceptions import NodeExistsError
        try:
            self.zk.create(self.base+'/leader', encoded({'coordinator_id': coordinator_id}),
                           ephemeral=True)
        except NodeExistsError:
            return None
        previous, stat = self.read('epoch')
        epoch = previous['epoch']+1
        current = self.zk.set(self.base+'/epoch', encoded({'epoch': epoch,
                              'coordinator_id': coordinator_id}), version=stat.version)
        return {'epoch': epoch, 'epoch_version': current.version,
                'coordinator_id': coordinator_id}

    def fenced_set(self, name, value, token):
        from kazoo.exceptions import BadVersionError
        transaction = self.zk.transaction()
        transaction.check(self.base+'/epoch', version=token['epoch_version'])
        transaction.set_data(self.base+'/'+name, encoded(value))
        results = transaction.commit()
        # Kazoo multi returns exceptions as values. Prefer the actual version error.
        for item in results:
            if isinstance(item, BadVersionError):
                raise item
        for item in results:
            if isinstance(item, Exception):
                raise item
        return results[1]

    def publish(self, value, token, mode):
        if mode == 'fenced':
            return self.fenced_set('result', value, token)
        if mode != 'unsafe':
            raise ValueError('Unknown publication mode')
        return self.zk.set(self.base+'/result', encoded(value), version=-1)


def connect(hosts, event=lambda **kw: None):
    from kazoo.client import KazooClient
    # timeout is the requested session lifetime; start(timeout=...) is a different limit.
    client = KazooClient(hosts=hosts, timeout=10.0, read_only=False,
                         command_retry={'max_tries': 3, 'delay': .2, 'max_delay': 1})
    # Kazoo 2.10 keeps _session_timeout as the REQUESTED value. Capture only the
    # non-secret timeout from the Connect response; never log the response/password.
    # This small private hook is version-pinned and checked by the live tests.
    negotiated = {}
    invoke = client._connection._invoke
    def capture_connect_response(*args, **kwargs):
        response = invoke(*args, **kwargs)
        if isinstance(response, tuple) and hasattr(response[0], 'time_out'):
            negotiated['milliseconds'] = response[0].time_out
        return response
    client._connection._invoke = capture_connect_response
    client.add_listener(lambda state: event(event='session_state', state=state))
    client.start(timeout=20)
    negotiated_ms = negotiated.get('milliseconds')
    if negotiated_ms != 10000:
        client.stop()
        client.close()
        raise ValueError(f'Negotiated session timeout is {negotiated_ms}, expected 10000 ms')
    event(event='session_connected', session_id=hex(client.client_id[0]),
          requested_session_timeout_ms=10000, negotiated_session_timeout_ms=negotiated_ms)
    return client
