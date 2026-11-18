import asyncio
import logging
from contextlib import asynccontextmanager, suppress
import os,uuid,datetime,json,sqlite3,hashlib,hmac,base64,secrets,time
from fastapi import Depends,Header
import aio_pika
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from pydantic import BaseModel,Field
from .common import call
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    async def recover_pending():
        while True:
            await asyncio.sleep(15)
            try:
                pending = await collection('request:')
                await publish_requests([x['id'] for x in pending if x['status'] == 'PENDING'])
            except Exception:
                logger.warning('Pending requests could not be queued this pass', exc_info=True)

    task = asyncio.create_task(recover_pending())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app=FastAPI(title='Emergency Response API',version='1.0.0',lifespan=lifespan)
FRONTEND=Path(__file__).resolve().parent.parent/'frontend'
COORD=os.getenv('COORDINATOR_URL','http://coordinator:8000')
MQ=os.getenv('RABBITMQ_URL','amqp://guest:guest@rabbitmq/')
AUTH_DB=os.getenv('AUTH_DB','/data/auth.sqlite')
os.makedirs(os.path.dirname(AUTH_DB),exist_ok=True)
with sqlite3.connect(AUTH_DB) as db:
    db.execute('CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, salt TEXT NOT NULL, password_hash TEXT NOT NULL, role TEXT NOT NULL)')
SECRET=os.getenv('AUTH_SECRET','')
if not SECRET: raise RuntimeError('AUTH_SECRET must be set')
ROLES={'citizen','volunteer','admin'}
class Signup(BaseModel):
    username:str=Field(min_length=3,max_length=60)
    password:str=Field(min_length=10)
    role:str=Field(pattern='^(citizen|volunteer)$')
class Login(BaseModel):
    username:str
    password:str

def password_hash(password,salt):return hashlib.pbkdf2_hmac('sha256',password.encode(),bytes.fromhex(salt),210000).hex()
def sign(payload):
    data=base64.urlsafe_b64encode(json.dumps(payload,separators=(',',':')).encode()).decode().rstrip('=')
    sig=hmac.new(SECRET.encode(),data.encode(),hashlib.sha256).hexdigest()
    return data+'.'+sig
def verify(token):
    try:
        data,sig=token.split('.')
        if not hmac.compare_digest(sig,hmac.new(SECRET.encode(),data.encode(),hashlib.sha256).hexdigest()):raise ValueError()
        obj=json.loads(base64.urlsafe_b64decode(data+'='*(-len(data)%4)))
        if obj['exp']<time.time():raise ValueError()
        return obj
    except Exception:raise HTTPException(401,'invalid or expired token')
def current_user(authorization:str=Header(default='')):
    if not authorization.startswith('Bearer '):raise HTTPException(401,'authentication required')
    user=verify(authorization[7:])
    with sqlite3.connect(AUTH_DB) as db:row=db.execute('SELECT role FROM users WHERE id=?',(user['sub'],)).fetchone()
    if not row:raise HTTPException(401,'user no longer exists')
    return {'id':user['sub'],'role':row[0]}
def allowed(*roles):
    def check(user=Depends(current_user)):
        if user['role'] not in roles:raise HTTPException(403,'insufficient permissions')
        return user
    return check
@app.post('/auth/signup',status_code=201)
def signup(data:Signup):
    uid=str(uuid.uuid4());salt=secrets.token_hex(16)
    try:
        with sqlite3.connect(AUTH_DB) as db:db.execute('INSERT INTO users VALUES (?,?,?,?,?)',(uid,data.username,salt,password_hash(data.password,salt),data.role))
    except sqlite3.IntegrityError:raise HTTPException(409,'username already exists')
    return {'id':uid,'role':data.role}
@app.post('/auth/login')
def login(data:Login):
    with sqlite3.connect(AUTH_DB) as db:row=db.execute('SELECT id,salt,password_hash,role FROM users WHERE username=?',(data.username,)).fetchone()
    if not row or not hmac.compare_digest(password_hash(data.password,row[1]),row[2]):raise HTTPException(401,'invalid credentials')
    return {'access_token':sign({'sub':row[0],'exp':int(time.time())+8*3600}),'token_type':'bearer','role':row[3]}
@app.get('/auth/me')
def me(user=Depends(current_user)):return user
class RequestIn(BaseModel):
    type:str=Field(pattern='^(food|water|medicine|transportation|shelter)$')
    location:str=Field(min_length=2)
    details:str=""
class ResourceIn(BaseModel):
    type:str=Field(pattern='^(food|water|medicine|transportation|shelter)$')
    location:str=Field(min_length=2)
    availability:bool=True
class StatusIn(BaseModel):status:str
async def store(kind,id,obj):
    r=await call('PUT',f'{COORD}/internal/data/{kind}:{id}',json={'value':obj,'version':obj.get('version',1)})
    if r.status_code!=200:raise HTTPException(503,'storage unavailable')
async def load(kind,id):
    r=await call('GET',f'{COORD}/internal/data/{kind}:{id}')
    if r.status_code!=200:raise HTTPException(r.status_code,'record not found or unavailable')
    return r.json()['value']
async def collection(prefix):
    response = await call('GET', f'{COORD}/internal/data', params={'prefix': prefix})
    if response.status_code != 200:
        raise HTTPException(503, 'storage unavailable')
    return [item['value'] for item in response.json()['items']]


async def publish_requests(ids, *, required=False):
    if not ids:
        return
    try:
        async with asyncio.timeout(4):
            connection = await aio_pika.connect_robust(MQ, timeout=3)
            async with connection:
                channel = await connection.channel(publisher_confirms=True)
                queue = await channel.declare_queue('match_requests', durable=True)
                for request_id in ids:
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps({'request_id': request_id}).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=queue.name,
                    )
    except Exception as exc:
        if required:
            raise HTTPException(503, 'matching queue unavailable') from exc
        logger.warning('Matching publication deferred; pending requests will be retried')


async def retry_pending_for_resource(resource):
    if not resource.get('verified') or not resource.get('availability'):
        return
    try:
        pending = await collection('request:')
    except HTTPException:
        return  # The periodic recovery pass checks persisted requests.
    await publish_requests([
        item['id'] for item in pending
        if item['status'] == 'PENDING' and item['type'] == resource['type']
        and item['location'].strip().casefold() == resource['location'].strip().casefold()
    ])


async def update_record(kind, id, **changes):
    response = await call('PATCH', f'{COORD}/internal/data/{kind}:{id}', json=changes)
    if response.status_code != 200:
        raise HTTPException(response.status_code, response.json().get('detail', 'storage error'))
    return response.json()


@app.get('/health')
def health():return {'ok':True}
@app.post('/requests',status_code=201)
async def create_request(data:RequestIn,user=Depends(allowed("citizen","admin"))):
    id=str(uuid.uuid4());obj={**data.model_dump(),'id':id,'status':'PENDING','created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'assigned_resource':None,'version':1,'owner_id':user['id']}
    await store('request',id,obj)
    await publish_requests([id])
    return obj
@app.get('/requests/{id}')
async def get_request(id:str,user=Depends(current_user)):
    obj=await load('request',id)
    if user['role'] == 'citizen' and obj.get('owner_id') != user['id']:
        raise HTTPException(403, 'not your request')
    if user['role'] == 'volunteer':
        owned = await list_owned_resources(user)
        if obj.get('assigned_resource') not in {r['id'] for r in owned}:
            raise HTTPException(403, 'request is not assigned to your resources')
    return obj
@app.put('/requests/{id}/status')
async def update_request(id:str,data:StatusIn,user=Depends(allowed("admin"))):
    return await update_record('request', id, status=data.status)
@app.post('/resources',status_code=201)
async def register_resource(data:ResourceIn,user=Depends(allowed("volunteer","admin"))):
    id=str(uuid.uuid4());obj={**data.model_dump(),'id':id,'status':'AVAILABLE' if data.availability else 'UNAVAILABLE','version':1,'owner_id':user['id'],'verified':True}
    await store('resource',id,obj)
    await retry_pending_for_resource(obj)
    return obj
@app.get('/resources')
async def list_resources(user=Depends(current_user)):
    if user['role'] == 'admin':
        return {'resources': await collection('resource:')}
    if user['role'] == 'volunteer':
        return {'resources': await list_owned_resources(user)}
    return {'resources': []}
@app.put('/resources/{id}/status')
async def update_resource(id:str,data:StatusIn,user=Depends(allowed("volunteer","admin"))):
    owner_id = user['id'] if user['role'] == 'volunteer' else None
    obj = await update_record('resource', id, status=data.status, owner_id=owner_id)
    await retry_pending_for_resource(obj)
    return obj
@app.post('/match/{request_id}')
async def match_request(request_id:str,user=Depends(allowed("admin"))):
    await load('request', request_id)
    await publish_requests([request_id], required=True)
    return {'request_id': request_id, 'status': 'QUEUED'}

@app.get('/requests')
async def list_requests(user=Depends(current_user)):
    requests = await collection('request:')
    if user['role'] == 'citizen':
        requests = [x for x in requests if x.get('owner_id') == user['id']]
    if user['role'] == 'volunteer':
        owned = {x['id'] for x in await list_owned_resources(user)}
        requests = [x for x in requests if x.get('assigned_resource') in owned]
    return {'requests': requests}


async def list_owned_resources(user):
    return [x for x in await collection('resource:') if x.get('owner_id') == user['id']]


@app.put('/resources/{id}/verify')
async def verify_resource(id:str,user=Depends(allowed('admin'))):
    obj = await update_record('resource', id, verify=True)
    await retry_pending_for_resource(obj)
    return obj
@app.get('/system/metrics')
async def metrics(user=Depends(allowed('admin'))):
    req = await collection('request:')
    total=len(req);matched=sum(x['status'] in ('MATCHED', 'COMPLETED') for x in req)
    return {'total_requests':total,'matched_requests':matched,'match_rate':round(matched/total,4) if total else None}
@app.get('/system/health')
async def system_health(user=Depends(allowed("admin"))):
    r=await call('GET',f'{COORD}/internal/nodes/status')
    if r.status_code!=200:raise HTTPException(503,'coordinator unavailable')
    return r.json()

@app.post('/system/reconcile')
async def system_reconcile(user=Depends(allowed("admin"))):
    r=await call('POST',f'{COORD}/internal/reconcile')
    if r.status_code!=200:raise HTTPException(503,'coordinator unavailable')
    return r.json()

@app.get('/')
async def dashboard():return FileResponse(FRONTEND/'index.html')
app.mount('/static',StaticFiles(directory=str(FRONTEND)),name='static')
