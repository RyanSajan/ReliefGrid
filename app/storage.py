import asyncio, json, logging, os, sqlite3
from contextlib import asynccontextmanager, suppress
from fastapi import FastAPI,HTTPException
from pydantic import BaseModel
from .common import call

NODE_NAME = os.getenv('NODE_NAME', 'unknown')
COORD = os.getenv('COORDINATOR_URL', 'http://coordinator:8000')
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    async def report():
        endpoint = 'register'
        while True:
            try:
                response = await call('POST', f'{COORD}/internal/nodes/{endpoint}',
                                      json={'name': NODE_NAME}, timeout=2)
                response.raise_for_status()
                endpoint = 'heartbeat'
            except Exception:
                endpoint = 'register'
                logger.debug('Could not report storage heartbeat', exc_info=True)
            await asyncio.sleep(2)

    task = asyncio.create_task(report())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app=FastAPI(title='Storage Node', lifespan=lifespan)
DB=os.getenv('DB_PATH','/data/store.sqlite')
os.makedirs(os.path.dirname(DB),exist_ok=True)
with sqlite3.connect(DB) as c: c.execute('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL)')
class Record(BaseModel):
    value:dict
    version:int=1
@app.get('/health')
def health(): return {'ok':True,'node':os.getenv('NODE_NAME','unknown')}
@app.get('/internal/data/{key}')
def get(key:str):
    with sqlite3.connect(DB) as c: row=c.execute('SELECT value,version FROM kv WHERE key=?',(key,)).fetchone()
    if not row: raise HTTPException(404,'not found')
    return {'value':json.loads(row[0]),'version':row[1]}
@app.put('/internal/data/{key}')
def put(key:str,record:Record):
    with sqlite3.connect(DB) as c:
        encoded = json.dumps(record.value, sort_keys=True)
        result = c.execute(
            'INSERT INTO kv VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET '
            'value=excluded.value,version=excluded.version WHERE excluded.version > kv.version',
            (key, encoded, record.version),
        )
        if not result.rowcount:
            value, version = c.execute('SELECT value,version FROM kv WHERE key=?', (key,)).fetchone()
            if version != record.version or json.loads(value) != record.value:
                raise HTTPException(409, 'stale or conflicting version')
    return {'ok':True}
@app.delete('/internal/data/{key}')
def delete(key:str):
    with sqlite3.connect(DB) as c:c.execute('DELETE FROM kv WHERE key=?',(key,))
    return {'ok':True}

@app.get('/internal/data')
def list_data(prefix:str=''):
    with sqlite3.connect(DB) as c:
        rows=c.execute('SELECT key,value,version FROM kv WHERE key LIKE ? ORDER BY key',(prefix+'%',)).fetchall()
    return {'items':[{'key':k,'value':json.loads(v),'version':n} for k,v,n in rows]}
