"""Run real Kubernetes experiments and retain their manifests, statuses and logs."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time

from manage import LAB, REPO, WORKSPACE, DATA, RUNTIME, save, prepare


def credentials():
    raw = subprocess.run([str(WORKSPACE/'bin/rc.exe'), 'alias', 'export', 'local',
                          '--include-credentials', '--acknowledge-credentials'],
                         capture_output=True, check=True, encoding='utf-8')
    return json.loads(raw.stdout)['aliases'][0]['credentials']


def client(endpoint, access):
    import boto3
    from botocore.config import Config
    return boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                        aws_access_key_id=access['access_key'], aws_secret_access_key=access['secret_key'],
                        config=Config(s3={'addressing_style': 'path'}, connect_timeout=5, read_timeout=30))


class Run:
    def __init__(self, stage):
        state = RUNTIME/'run.json'
        if state.exists():
            self.id = json.loads(state.read_text())['id']
        else:
            self.id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            save(state, {'id': self.id})
        self.folder = LAB/'evidence'/self.id/stage
        self.folder.mkdir(parents=True, exist_ok=True)
        self.sequence = max([int(p.name.split('-')[0]) for p in self.folder.glob('*-command.json')]+[0])

    def kubectl(self, *args, data=None, check=True, record=True):
        kubectl = RUNTIME/'minikube/.minikube/cache/windows/amd64/v1.37.0/kubectl.exe'
        command = [str(kubectl), '--context=de-lab5', *map(str,args)]
        result = subprocess.run(command, input=None if data is None else json.dumps(data),
                                capture_output=True, encoding='utf-8', errors='replace',
                                env={**os.environ,'MINIKUBE_HOME':str(RUNTIME/'minikube')})
        if record:
            self.sequence += 1
            save(self.folder/f'{self.sequence:03d}-command.json', {
                'at_utc': datetime.now(timezone.utc).isoformat(), 'command': command,
                'exit_code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        if result.returncode and check:
            raise RuntimeError(result.stderr)
        return result

    def apply(self, value, name):
        # Secrets are sent through stdin and never saved as manifests or command output.
        if value['kind'] != 'Secret':
            save(self.folder/(name+'.json'), value)
        return self.kubectl('apply', '-f', '-', data=value)

    def job(self, name, limit, endpoint, prefix, mode='map', parallel=False):
        args = ['--prefix', prefix, '--mode', mode]
        container = {'name': 'calculate', 'image': 'de-lab5:local', 'imagePullPolicy': 'Never',
                     'args': args, 'resources': {'requests': {'cpu':'500m', 'memory':f'{min(128,limit)}Mi'},
                                                'limits': {'cpu':'1000m', 'memory':f'{limit}Mi'}},
                     'envFrom':[{'secretRef':{'name':'lab5-s3'}}],
                     'env':[{'name':'LAB5_S3_ENDPOINT','value':endpoint}]}
        spec = {'backoffLimit':0, 'template':{'metadata':{'labels':{'lab5-run':self.id}},
                'spec':{'restartPolicy':'Never','containers':[container]}}}
        if parallel:
            spec.update(completionMode='Indexed', completions=4, parallelism=2, backoffLimit=2)
            container['env'].append({'name':'JOB_COMPLETION_INDEX','valueFrom':{'fieldRef':{
                'fieldPath':"metadata.annotations['batch.kubernetes.io/job-completion-index']"}}})
        self.apply({'apiVersion':'batch/v1','kind':'Job',
                    'metadata':{'name':name,'namespace':'lab5'},'spec':spec}, name+'-manifest')
        until = time.monotonic()+180
        while time.monotonic()<until:
            result = self.kubectl('-n','lab5','get','job',name,'-o','json',record=False)
            job = json.loads(result.stdout)
            conditions = job.get('status',{}).get('conditions',[])
            if any(c['type'] in ['Complete','Failed'] and c['status']=='True' for c in conditions):
                break
            time.sleep(2)
        else:
            raise TimeoutError('Job timeout: '+name)
        pods = json.loads(self.kubectl('-n','lab5','get','pods','-l','job-name='+name,'-o','json').stdout)
        for pod in pods['items']:
            self.kubectl('-n','lab5','logs',pod['metadata']['name'],check=False)
            self.kubectl('-n','lab5','describe','pod',pod['metadata']['name'])
        self.kubectl('-n','lab5','get','events','-o','json')
        save(self.folder/(name+'-status.json'), {'job':job,'pods':pods})
        return job,pods


def host_storage():
    access = credentials()
    try:
        with socket.create_connection(('127.0.0.1',9000),timeout=1): pass
    except OSError:
        env = {**os.environ,'RUSTFS_ERASURE_SET_DRIVE_COUNT':'6','RUSTFS_STORAGE_CLASS_STANDARD':'EC:3',
               'RUSTFS_UNSAFE_BYPASS_DISK_CHECK':'true','RUSTFS_ACCESS_KEY':access['access_key'],
               'RUSTFS_SECRET_KEY':access['secret_key']}
        RUNTIME.mkdir(parents=True,exist_ok=True)
        with (RUNTIME/'host-rustfs.stdout.log').open('ab') as out, (RUNTIME/'host-rustfs.stderr.log').open('ab') as err:
            process = subprocess.Popen([str(WORKSPACE/'bin/rustfs.exe'),'server','--address','0.0.0.0:9000',
                '--console-enable','--console-address','127.0.0.1:9001',
                *[str(WORKSPACE/'disks'/f'disk{i}') for i in range(1,7)]],
                env=env,stdout=out,stderr=err,cwd=WORKSPACE,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        save(RUNTIME/'host-rustfs.json', {'pid':process.pid,'started_by_lab5':True})
    s3=client('http://127.0.0.1:9000',access)
    for _ in range(60):
        try:
            s3.head_bucket(Bucket='hh-lake')
            return s3,access
        except Exception:
            time.sleep(0.5)
    raise RuntimeError('Original S3 storage is not ready')


def upload(s3):
    records=[]
    for p in DATA.rglob('*'):
        if not p.is_file() or '/results/' in p.as_posix(): continue
        key=p.relative_to(DATA).as_posix()
        if key.startswith('lab5/input/') or key.startswith('raw/'):
            payload=p.read_bytes()
            if key.startswith('lab5/'):
                s3.put_object(Bucket='hh-lake',Key=key,Body=payload)
            remote=s3.get_object(Bucket='hh-lake',Key=key)['Body'].read()
            assert hashlib.sha256(remote).digest()==hashlib.sha256(payload).digest(),key
            records.append({'key':key,'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest()})
    return records


def bootstrap(run):
    prepare()
    s3,access=host_storage()
    run.apply({'apiVersion':'v1','kind':'Namespace','metadata':{'name':'lab5'}},'namespace')
    run.apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':'lab5-s3','namespace':'lab5'},
               'type':'Opaque','stringData':{'LAB5_S3_ACCESS_KEY':access['access_key'],
                                           'LAB5_S3_SECRET_KEY':access['secret_key']}},'secret')
    save(run.folder/'source-objects.json',upload(s3))
    return s3


def summary(s3,prefix,index='full'):
    return json.loads(s3.get_object(Bucket='hh-lake',Key=f'{prefix}/{index}/summary.json')['Body'].read())


def memory(run):
    s3=bootstrap(run)
    endpoint='http://host.minikube.internal:9000'
    values=[]
    for i in range(3):
        name=f'base-{run.id.lower()}-{i}';prefix=f'lab5/runs/{run.id}/baseline-{i}'
        run.job(name,512,endpoint,prefix)
        value=summary(s3,prefix)
        assert value['memory_peak']['bytes'] is not None
        values.append(value)
    oom=None
    for limit in [64,48,32]:
        name=f'oom-{run.id.lower()}-{limit}'
        job,pods=run.job(name,limit,endpoint,f'lab5/runs/{run.id}/oom-{limit}')
        terminated=[c.get('state',{}).get('terminated',{}) for p in pods['items']
                    for c in p.get('status',{}).get('containerStatuses',[])]
        if any(t.get('reason')=='OOMKilled' for t in terminated):
            oom={'limit_mib':limit,'job':name,'terminated':terminated};break
    assert oom is not None,'No actual OOMKilled was observed'
    peak=max(v['memory_peak']['bytes'] for v in values)
    selected=math.ceil(peak*1.25/(16*1048576))*16
    successful=[]
    for i in range(3):
        prefix=f'lab5/runs/{run.id}/selected-{i}'
        run.job(f'safe-{run.id.lower()}-{i}',selected,endpoint,prefix)
        successful.append(summary(s3,prefix))
    save(run.folder/'memory-selection.json',{'baseline':values,'oom':oom,'maximum_peak_bytes':peak,
        'formula':'ceil(maximum_peak_bytes * 1.25 / (16 MiB)) * 16 MiB',
        'selected_limit_mib':selected,'successful':successful})
    print('Observed OOMKilled; selected memory:',selected,'MiB',flush=True)


@contextmanager
def forward():
    kubectl=RUNTIME/'minikube/.minikube/cache/windows/amd64/v1.37.0/kubectl.exe'
    with (RUNTIME/'s3-forward.log').open('ab') as log:
        process=subprocess.Popen([str(kubectl),'--context=de-lab5','-n','lab5','port-forward',
                                  'service/rustfs','19000:9000','--address=127.0.0.1'],
                                 stdout=log,stderr=log,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    try:
        for _ in range(60):
            if process.poll() is not None: raise RuntimeError('S3 port-forward failed')
            try:
                with socket.create_connection(('127.0.0.1',19000),timeout=0.2): break
            except OSError: time.sleep(0.3)
        else: raise TimeoutError('S3 port-forward not ready')
        yield
    finally:
        process.terminate()
        try: process.wait(timeout=5)
        except subprocess.TimeoutExpired: process.kill()


def migrate(run):
    source,access=host_storage()
    run.kubectl('apply','-f',LAB/'manifests/storage.yaml')
    run.kubectl('-n','lab5','rollout','status','deployment/rustfs','--timeout=150s')
    records=json.loads((LAB/'evidence'/run.id/'memory/source-objects.json').read_text(encoding='utf-8'))
    target=client('http://127.0.0.1:19000',access)
    copied=[]
    with forward():
        try: target.head_bucket(Bucket='hh-lake')
        except Exception: target.create_bucket(Bucket='hh-lake')
        for record in records:
            payload=source.get_object(Bucket='hh-lake',Key=record['key'])['Body'].read()
            target.put_object(Bucket='hh-lake',Key=record['key'],Body=payload)
            back=target.get_object(Bucket='hh-lake',Key=record['key'])['Body'].read()
            assert hashlib.sha256(back).hexdigest()==record['sha256']
            copied.append(record)
    run.kubectl('-n','lab5','rollout','restart','deployment/rustfs')
    run.kubectl('-n','lab5','rollout','status','deployment/rustfs','--timeout=150s')
    with forward():
        for record in records:
            back=target.get_object(Bucket='hh-lake',Key=record['key'])['Body'].read()
            assert hashlib.sha256(back).hexdigest()==record['sha256']
    run.kubectl('-n','lab5','get','deployment,service,pvc,pods','-o','json')
    save(run.folder/'migration.json',{'objects':copied,'all_sha256_equal':True,'survived_pod_restart':True,
        'internal_endpoint':'http://rustfs.lab5.svc.cluster.local:9000',
        'layout':'one persistent volume; no erasure coding; original six-disk storage preserved'})
    selection=json.loads((LAB/'evidence'/run.id/'memory/memory-selection.json').read_text(encoding='utf-8'))
    limit=selection['selected_limit_mib']
    prefix=f'lab5/runs/{run.id}/parallel'
    run.job('parallel-'+run.id.lower(),limit,'http://rustfs.lab5.svc.cluster.local:9000',prefix,parallel=True)
    run.job('merge-'+run.id.lower(),limit,'http://rustfs.lab5.svc.cluster.local:9000',prefix,mode='merge')
    with forward():
        result=summary(target,prefix,'merged')
        pairs=target.get_object(Bucket='hh-lake',Key=prefix+'/merged/pairs.csv')['Body'].read()
        assert pairs==(DATA/'lab5/input/reference.csv').read_bytes()
    (run.folder/'pairs.csv').write_bytes(pairs)
    save(run.folder/'parallel-verification.json',{'all_pairs_equal':True,'result':result})
    print('S3 migrated, persistence checked, parallel result equals lab3.',flush=True)


def milli(value):
    return int(value[:-1]) if value.endswith('m') else int(float(value)*1000)


def running(run,name):
    deadline=time.monotonic()+45
    while time.monotonic()<deadline:
        value=json.loads(run.kubectl('-n','lab5','get','pod',name,'-o','json',record=False).stdout)
        if value['status']['phase']=='Running': return value
        time.sleep(0.5)
    raise TimeoutError(name+' did not start')


def scheduling(run):
    node=json.loads(run.kubectl('get','nodes','-o','json').stdout)['items'][0]
    pods=json.loads(run.kubectl('get','pods','-A','-o','json').stdout)['items']
    reserved=0
    for pod in pods:
        if pod.get('spec',{}).get('nodeName')!=node['metadata']['name'] or pod['status']['phase'] in ['Succeeded','Failed']: continue
        regular=sum(milli(c.get('resources',{}).get('requests',{}).get('cpu','0')) for c in pod['spec']['containers'])
        init=max([milli(c.get('resources',{}).get('requests',{}).get('cpu','0')) for c in pod['spec'].get('initContainers',[])]+[0])
        reserved+=max(regular,init)
    available=milli(node['status']['allocatable']['cpu'])-reserved
    request=available-100
    assert request>=300 and 2*request>available
    for name,value,policy in [('lab5-low',100,'Never'),('lab5-high',1000,'PreemptLowerPriority')]:
        run.apply({'apiVersion':'scheduling.k8s.io/v1','kind':'PriorityClass',
                   'metadata':{'name':name},'value':value,'globalDefault':False,'preemptionPolicy':policy},name)
    def pod(name,priority):
        return {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':'lab5'},
                'spec':{'priorityClassName':priority,'restartPolicy':'Never','terminationGracePeriodSeconds':1,
                    'containers':[{'name':'hold','image':'de-lab5:local','imagePullPolicy':'Never',
                        'command':['python','-c',"import time; print('running', flush=True); time.sleep(120)"],
                        'resources':{'requests':{'cpu':f'{request}m','memory':'32Mi'},
                                     'limits':{'cpu':f'{request}m','memory':'64Mi'}}}]}}
    run.apply(pod('cpu-low','lab5-low'),'low-manifest')
    running(run,'cpu-low')
    run.apply(pod('cpu-waiting','lab5-low'),'waiting-manifest')
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        waiting=json.loads(run.kubectl('-n','lab5','get','pod','cpu-waiting','-o','json',record=False).stdout)
        if any('Insufficient cpu' in c.get('message','') for c in waiting['status'].get('conditions',[])): break
        time.sleep(0.5)
    else: raise AssertionError('No Insufficient cpu condition')
    save(run.folder/'waiting.json',waiting)
    run.kubectl('-n','lab5','get','events','-o','json')
    run.apply(pod('cpu-high','lab5-high'),'high-manifest')
    high=running(run,'cpu-high')
    events=json.loads(run.kubectl('-n','lab5','get','events','-o','json').stdout)
    preempted=[e for e in events['items'] if e.get('involvedObject',{}).get('name')=='cpu-low'
               and 'preempt' in (e.get('reason','')+' '+e.get('message','')).lower()]
    assert preempted,'No scheduler preemption event for the low priority victim'
    save(run.folder/'preemption.json',{'allocatable_cpu':node['status']['allocatable']['cpu'],
        'existing_requests_millicpu':reserved,'test_request_millicpu':request,
        'waiting':waiting,'high_running':high,'preemption_events':preempted})
    run.kubectl('-n','lab5','delete','pod','cpu-high','--wait=true')
    resumed=running(run,'cpu-waiting')
    save(run.folder/'waiting-started-after-release.json',resumed)
    run.kubectl('-n','lab5','delete','pod','cpu-waiting','--wait=true')
    print('Insufficient CPU, priority preemption, and subsequent scheduling observed.',flush=True)


def quota(run):
    run.kubectl('apply','-f',LAB/'manifests/quota.yaml')
    def pod(name):
        return {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':'lab5-quota'},
                'spec':{'restartPolicy':'Never','terminationGracePeriodSeconds':1,'containers':[{
                    'name':'hold','image':'de-lab5:local','imagePullPolicy':'Never',
                    'command':['python','-c','import time; time.sleep(120)'],
                    'resources':{'requests':{'cpu':'700m','memory':'32Mi'},
                                 'limits':{'cpu':'700m','memory':'64Mi'}}}]}}
    run.apply(pod('quota-one'),'quota-one')
    run.kubectl('-n','lab5-quota','get','resourcequota','lab5-budget','-o','json')
    denied=run.kubectl('apply','-f','-',data=pod('quota-two'),check=False)
    assert denied.returncode!=0 and 'exceeded quota' in denied.stderr.lower()
    save(run.folder/'quota-denied.json',{'exit_code':denied.returncode,'stderr':denied.stderr})
    run.kubectl('-n','lab5-quota','delete','pod','quota-one','--wait=true')
    for _ in range(15):
        retry=run.kubectl('apply','-f','-',data=pod('quota-two'),check=False)
        if retry.returncode==0: break
        time.sleep(0.5)
    else: raise AssertionError('Quota did not recover')
    run.kubectl('-n','lab5-quota','get','pods,resourcequota','-o','json')
    save(run.folder/'quota-verification.json',{'excess_rejected':True,'retry_after_release_accepted':True})
    run.kubectl('-n','lab5-quota','delete','pod','quota-two','--wait=true')
    print('Quota excess rejected; retry accepted after releasing quota.',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage',choices=['memory','migrate','scheduling','quota'])
    parser.add_argument('--new-run',action='store_true',help='Start a fresh run ID; use only with memory')
    args=parser.parse_args()
    if args.new_run:
        if args.stage!='memory': parser.error('--new-run must start with the memory stage')
        save(RUNTIME/'run.json',{'id':datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')})
    run=Run(args.stage)
    {'memory':memory,'migrate':migrate,'scheduling':scheduling,'quota':quota}[args.stage](run)


if __name__=='__main__': main()
