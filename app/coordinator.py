import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .common import call, shard

NODES = [
    tuple(os.getenv(f'SHARD_{i}_{role.upper()}', f'http://shard-{i}-{role}:8000')
          for role in ('primary', 'replica'))
    for i in range(3)
]
NODE_NAMES = {
    url: f'shard-{i}-{role}'
    for i, pair in enumerate(NODES)
    for role, url in zip(('primary', 'replica'), pair)
}
HEARTBEAT_TIMEOUT = 8
last_seen = {}
mutation_lock = asyncio.Lock()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    async def repair_loop():
        while True:
            try:
                await reconcile()
            except Exception:
                logger.exception('Storage reconciliation failed')
            await asyncio.sleep(5)

    task = asyncio.create_task(repair_loop())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title='Storage Coordinator', lifespan=lifespan)


class Record(BaseModel):
    value: dict
    version: int = 1


class Update(BaseModel):
    status: str | None = None
    verify: bool = False
    owner_id: str | None = None


def healthy(node):
    seen = last_seen.get(NODE_NAMES[node])
    return seen is not None and time.monotonic() - seen < HEARTBEAT_TIMEOUT


@app.post('/internal/nodes/register')
@app.post('/internal/nodes/heartbeat')
async def heartbeat(payload: dict):
    name = payload.get('name')
    if name not in NODE_NAMES.values():
        raise HTTPException(422, 'unknown storage node')
    last_seen[name] = time.monotonic()
    return {'ok': True}


@app.get('/health')
async def health():
    return {'ok': True}


@app.get('/internal/shards')
async def shards():
    return {'shards': [
        {'id': i, 'primary': primary, 'replica': replica}
        for i, (primary, replica) in enumerate(NODES)
    ]}


@app.get('/internal/nodes/status')
async def node_status():
    return {'nodes': [
        {'shard': i, 'role': role, 'healthy': healthy(node)}
        for i, pair in enumerate(NODES)
        for role, node in zip(('primary', 'replica'), pair)
    ]}


async def node_call(method, node, path, **kwargs):
    try:
        return await call(method, node + path, timeout=2, **kwargs)
    except HTTPException as exc:
        if exc.status_code != 503:
            raise
        return None


async def read_copies(index, path, **kwargs):
    responses = await asyncio.gather(*(
        node_call('GET', node, path, **kwargs)
        for node in NODES[index] if healthy(node)
    ))
    good = [r for r in responses if r is not None and r.status_code in (200, 404)]
    if not good:
        raise HTTPException(503, 'shard unavailable')
    return good


async def newest(key):
    copies = await read_copies(shard(key), '/internal/data/' + key)
    records = [r.json() for r in copies if r.status_code == 200]
    return max(records, key=lambda record: record['version'], default=None)


@app.get('/internal/data/{key}')
async def get(key: str):
    record = await newest(key)
    if record is None or record['value'].get('_deleted'):
        raise HTTPException(404, 'record not found')
    return record


async def write(key, record):
    index = shard(key)
    results = await asyncio.gather(*(
        node_call('PUT', node, '/internal/data/' + key, json=record.model_dump())
        for node in NODES[index] if healthy(node)
    ))
    if any(r is not None and r.status_code == 200 for r in results):
        return {'ok': True, 'shard': index}
    if any(r is not None and r.status_code == 409 for r in results):
        raise HTTPException(409, 'stale record')
    raise HTTPException(503, 'shard unavailable')


@app.put('/internal/data/{key}')
async def put(key: str, record: Record):
    async with mutation_lock:
        return await write(key, record)


@app.delete('/internal/data/{key}')
async def delete(key: str):
    async with mutation_lock:
        record = await newest(key)
        version = record['version'] + 1 if record else 1
        await write(key, Record(value={'_deleted': True}, version=version))
    return {'ok': True}


@app.get('/internal/data')
async def list_data(prefix: str = ''):
    shards = await asyncio.gather(*(
        read_copies(i, '/internal/data', params={'prefix': prefix}) for i in range(3)
    ))
    items = {}
    for copies in shards:
        for response in copies:
            if response.status_code != 200:
                continue
            for item in response.json()['items']:
                key = item['key']
                if key not in items or item['version'] > items[key]['version']:
                    items[key] = item
    return {'items': [item for item in items.values() if not item['value'].get('_deleted')]}


async def save_update(key, existing, **changes):
    version = existing['version'] + 1
    value = {**existing['value'], **changes, 'version': version}
    await write(key, Record(value=value, version=version))
    return value


def request_finished(status):
    return status in ('COMPLETED', 'CANCELLED')


@app.patch('/internal/data/{key}')
async def update(key: str, update: Update):
    async with mutation_lock:
        record = await get(key)
        obj = record['value']
        if update.owner_id is not None and obj.get('owner_id') != update.owner_id:
            raise HTTPException(403, 'not your resource')
        if key.startswith('request:'):
            if update.status not in ('PENDING', 'MATCHED', 'COMPLETED', 'CANCELLED'):
                raise HTTPException(422, 'invalid request status')
            allowed = {
                'PENDING': {'PENDING', 'CANCELLED'},
                'MATCHED': {'MATCHED', 'COMPLETED', 'CANCELLED'},
                'COMPLETED': {'COMPLETED'},
                'CANCELLED': {'CANCELLED'},
            }
            if update.status not in allowed.get(obj['status'], {'PENDING', 'CANCELLED'}):
                raise HTTPException(409, 'invalid request status transition')
            return await save_update(key, record, status=update.status)
        if update.verify:
            return await save_update(key, record, verified=True)
        if update.status not in ('AVAILABLE', 'UNAVAILABLE'):
            raise HTTPException(422, 'invalid resource status')
        assigned = obj.get('assigned_request')
        if assigned:
            request = await newest('request:' + assigned)
            if request and not request['value'].get('_deleted') and not request_finished(request['value']['status']):
                raise HTTPException(409, 'resource has an active assignment')
        elif obj['status'] == 'ASSIGNED':
            # Assignments made before the reservation field was added.
            requests = (await list_data('request:'))['items']
            if any(item['value'].get('assigned_resource') == obj['id']
                   and not request_finished(item['value']['status']) for item in requests):
                raise HTTPException(409, 'resource has an active assignment')
        return await save_update(key, record, status=update.status,
                                 availability=update.status == 'AVAILABLE',
                                 assigned_request=None)


@app.post('/internal/claim/{request_id}')
async def claim(request_id: str):
    async with mutation_lock:
        key = 'request:' + request_id
        record = await get(key)
        req = record['value']
        if req['status'] != 'PENDING':
            return {'status': 'ALREADY_PROCESSED'}
        resources = sorted((await list_data('resource:'))['items'], key=lambda x: x['key'])
        reserved = next((x for x in resources if x['value'].get('assigned_request') == request_id), None)
        if reserved is None:
            reserved = next((
                x for x in resources
                if x['value']['type'] == req['type']
                and x['value']['location'].strip().casefold() == req['location'].strip().casefold()
                and x['value']['availability'] and x['value'].get('verified', False)
            ), None)
            if reserved is None:
                return {'status': 'NO_RESOURCE'}
            await save_update(reserved['key'], reserved, availability=False,
                              status='ASSIGNED', assigned_request=request_id)
        resource_id = reserved['value']['id']
        await save_update(key, record, assigned_resource=resource_id, status='MATCHED')
        return {'status': 'MATCHED', 'resource_id': resource_id}


@app.post('/internal/reconcile')
async def reconcile():
    repaired = 0
    failed = []
    async with mutation_lock:
        for i, (primary, replica) in enumerate(NODES):
            if not healthy(primary) or not healthy(replica):
                failed.append({'shard': i, 'error': 'storage node unavailable'})
                continue
            left, right = await asyncio.gather(*(
                node_call('GET', node, '/internal/data') for node in (primary, replica)
            ))
            if any(r is None or r.status_code != 200 for r in (left, right)):
                failed.append({'shard': i, 'error': 'could not read shard copies'})
                continue
            primary_records, replica_records = (
                {x['key']: x for x in response.json()['items']} for response in (left, right)
            )
            for key in primary_records.keys() | replica_records.keys():
                a, b = primary_records.get(key), replica_records.get(key)
                if a and (not b or a['version'] > b['version']):
                    target, record = replica, a
                elif b and (not a or b['version'] > a['version']):
                    target, record = primary, b
                else:
                    continue
                result = await node_call('PUT', target, '/internal/data/' + key,
                                         json=Record(**record).model_dump())
                if result is not None and result.status_code == 200:
                    repaired += 1
                else:
                    failed.append({'shard': i, 'error': f'could not repair {key}'})
    return {'repaired': repaired, 'failed': failed}

