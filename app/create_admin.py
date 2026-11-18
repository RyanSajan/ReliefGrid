import getpass,uuid,secrets,sqlite3
from .request_service import AUTH_DB,password_hash
username=input('Admin username: ').strip()
password=getpass.getpass('Admin password (10+ chars): ')
if len(password)<10:raise SystemExit('Password too short')
salt=secrets.token_hex(16)
with sqlite3.connect(AUTH_DB) as db:
    db.execute('INSERT INTO users VALUES (?,?,?,?,?)',(str(uuid.uuid4()),username,salt,password_hash(password,salt),'admin'))
print('Admin created')
