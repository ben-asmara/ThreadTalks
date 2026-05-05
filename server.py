"""
=============================================================
  ThreadTalk v7  —  Single Port, Render Free Tier
  Threads · Sockets · SQLite · Auth · WebSocket
=============================================================
  Uses aiohttp to serve BOTH HTTP REST and WebSocket
  on the same port — aiohttp natively supports this.
=============================================================
"""
import asyncio, sqlite3, json, sys, hashlib, secrets, os, threading
from datetime import datetime
from aiohttp import web
import aiohttp

PORT    = int(os.environ.get("PORT", 10000))
DB_FILE = os.environ.get("DB_FILE", "chat.db")
COLORS  = ["#6c63ff","#00e5c3","#f59e0b","#ec4899","#3b82f6",
           "#10b981","#f97316","#a855f7","#06b6d4","#84cc16"]

ws_clients      = {}
ws_clients_lock = threading.Lock()
sessions        = {}
sessions_lock   = threading.Lock()

_db_lock = threading.Lock()
_db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)
_db_conn.row_factory = sqlite3.Row

def db_exec(sql, p=()):
    with _db_lock:
        c = _db_conn.execute(sql, p); _db_conn.commit(); return c
def db_q(sql, p=()):
    with _db_lock: return _db_conn.execute(sql, p).fetchall()
def db_1(sql, p=()):
    with _db_lock: return _db_conn.execute(sql, p).fetchone()

def init_db():
    db_exec("""CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,username TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL,password_hash TEXT NOT NULL,
        color TEXT NOT NULL DEFAULT '#6c63ff',is_admin INTEGER NOT NULL DEFAULT 0,
        is_banned INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,
        last_seen TEXT,msg_count INTEGER DEFAULT 0)""")
    db_exec("""CREATE TABLE IF NOT EXISTS rooms(
        id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE NOT NULL,
        topic TEXT DEFAULT '',created_by TEXT DEFAULT 'system',created_at TEXT NOT NULL)""")
    db_exec("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,room TEXT NOT NULL,
        user_id INTEGER NOT NULL,user_name TEXT NOT NULL,text TEXT NOT NULL,sent_at TEXT NOT NULL)""")
    db_exec("""CREATE TABLE IF NOT EXISTS sessions(
        token TEXT PRIMARY KEY,user_id INTEGER NOT NULL,created_at TEXT NOT NULL)""")
    for n,t in [("general","General chat"),("random","Anything goes"),("tech","Tech talk")]:
        db_exec("INSERT OR IGNORE INTO rooms(name,topic,created_at)VALUES(?,?,?)",(n,t,now()))
    if not db_1("SELECT id FROM users WHERE username='admin'"):
        db_exec("INSERT INTO users(username,display_name,password_hash,color,is_admin,created_at)VALUES(?,?,?,?,1,?)",
                ("admin","Administrator",hpw("admin123"),"#6c63ff",now()))
        print("[db] Admin → admin/admin123")
    print(f"[db] Ready ✓ {DB_FILE}")

def now():    return datetime.now().isoformat(timespec="seconds")
def ts():     return datetime.now().strftime("%H:%M")
def hpw(pw):  return hashlib.sha256(pw.encode()).hexdigest()
def ntok():   return secrets.token_hex(32)
def color():
    n=db_q("SELECT COUNT(*) as n FROM users")[0]["n"]; return COLORS[n%len(COLORS)]

def resolve(token):
    if not token: return None
    with sessions_lock:
        if token in sessions: return sessions[token]
    row=db_1("SELECT u.* FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?",(token,))
    if not row: return None
    info=dict(row)
    with sessions_lock: sessions[token]=info
    return info

def history(room,limit=40):
    rows=db_q("SELECT m.user_name,m.text,m.sent_at,u.color FROM messages m "
              "LEFT JOIN users u ON m.user_id=u.id WHERE m.room=? ORDER BY m.id DESC LIMIT ?",(room,limit))
    return list(reversed([dict(r) for r in rows]))

def rooms():  return [dict(r) for r in db_q("SELECT name,topic FROM rooms ORDER BY name")]
def lboard(): return [dict(r) for r in db_q("SELECT display_name as name,color,msg_count FROM users ORDER BY msg_count DESC LIMIT 10")]
def stats():
    return {"messages":db_1("SELECT COUNT(*) as c FROM messages")["c"],
            "users":db_1("SELECT COUNT(*) as c FROM users")["c"],
            "rooms":db_1("SELECT COUNT(*) as c FROM rooms")["c"]}
def save_msg(room,uid,uname,text):
    db_exec("INSERT INTO messages(room,user_id,user_name,text,sent_at)VALUES(?,?,?,?,?)",(room,uid,uname,text,now()))
    db_exec("UPDATE users SET msg_count=msg_count+1,last_seen=? WHERE id=?",(now(),uid))

def ulist(room=None):
    with ws_clients_lock:
        return [{"name":v["name"],"color":v["color"]} for v in ws_clients.values()
                if room is None or v.get("room")==room]

async def bcast(payload,room=None,exclude=None):
    msg=json.dumps(payload)
    with ws_clients_lock:
        targets=[ws for ws,info in ws_clients.items()
                 if ws is not exclude and (room is None or info.get("room")==room)]
    dead=[]
    for ws in targets:
        try: await ws.send_str(msg)
        except: dead.append(ws)
    for ws in dead:
        with ws_clients_lock: ws_clients.pop(ws,None)

# ─── CORS middleware ───────────────────────────────────────────────────────────
@web.middleware
async def cors(req, handler):
    if req.method == "OPTIONS":
        resp = web.Response()
    else:
        try:    resp = await handler(req)
        except web.HTTPException as e: resp = e
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return resp

def J(data, status=200): return web.Response(text=json.dumps(data), status=status, content_type="application/json")
def gtok(req): return req.headers.get("Authorization","").replace("Bearer ","").strip()

# ─── Auth routes ───────────────────────────────────────────────────────────────
async def login(req):
    d=await req.json()
    uname=(d.get("username") or "").strip().lower()
    user=db_1("SELECT * FROM users WHERE username=? AND password_hash=?",(uname,hpw(d.get("password",""))))
    if not user: return J({"error":"Invalid username or password."},401)
    user=dict(user)
    if user.get("is_banned"): return J({"error":"Account banned."},403)
    tok=ntok()
    db_exec("INSERT INTO sessions(token,user_id,created_at)VALUES(?,?,?)",(tok,user["id"],now()))
    db_exec("UPDATE users SET last_seen=? WHERE id=?",(now(),user["id"]))
    with sessions_lock: sessions[tok]=user
    print(f"[auth] → {user['display_name']}")
    return J({"token":tok,"user":{"id":user["id"],"username":user["username"],
        "display_name":user["display_name"],"color":user["color"],"is_admin":bool(user["is_admin"])}})

async def register(req):
    d=await req.json()
    uname=(d.get("username") or "").strip().lower()[:30]
    dname=(d.get("display_name") or uname).strip()[:30]
    pw=d.get("password","")
    if not uname or not pw: return J({"error":"Username and password required."},400)
    if len(pw)<6: return J({"error":"Password must be at least 6 characters."},400)
    if db_1("SELECT id FROM users WHERE username=?",(uname,)): return J({"error":"Username already taken."},409)
    c=color()
    db_exec("INSERT INTO users(username,display_name,password_hash,color,created_at)VALUES(?,?,?,?,?)",(uname,dname,hpw(pw),c,now()))
    user=dict(db_1("SELECT * FROM users WHERE username=?",(uname,)))
    tok=ntok()
    db_exec("INSERT INTO sessions(token,user_id,created_at)VALUES(?,?,?)",(tok,user["id"],now()))
    print(f"[auth] + {dname}")
    return J({"token":tok,"user":{"id":user["id"],"username":uname,"display_name":dname,"color":c,"is_admin":False}},201)

async def logout(req):
    tok=gtok(req); db_exec("DELETE FROM sessions WHERE token=?",(tok,))
    with sessions_lock: sessions.pop(tok,None)
    return J({"ok":True})

async def me(req):
    u=resolve(gtok(req))
    if not u: return J({"error":"Not authenticated."},401)
    return J({"user":{"id":u["id"],"username":u["username"],"display_name":u["display_name"],
        "color":u["color"],"is_admin":bool(u["is_admin"]),"msg_count":u["msg_count"]}})

# ─── Admin routes ──────────────────────────────────────────────────────────────
async def admin_stats(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    s=stats(); s["online"]=len(ws_clients)
    return J({"stats":s,"leaderboard":lboard()})

async def admin_users(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    return J({"users":[dict(r) for r in db_q("SELECT id,username,display_name,color,is_admin,is_banned,created_at,last_seen,msg_count FROM users ORDER BY created_at DESC")]})

async def admin_messages(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    return J({"messages":[dict(r) for r in db_q("SELECT id,room,user_name,text,sent_at FROM messages ORDER BY id DESC LIMIT 100")]})

async def admin_rooms_get(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    return J({"rooms":[dict(r) for r in db_q("SELECT r.*,(SELECT COUNT(*) FROM messages m WHERE m.room=r.name) as msg_count FROM rooms r ORDER BY r.name")]})

async def admin_rooms_post(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    d=await req.json()
    name=(d.get("name") or "").strip().lower().replace(" ","-")[:20]
    topic=(d.get("topic") or "").strip()[:80]
    if not name: return J({"error":"Room name required."},400)
    if db_1("SELECT id FROM rooms WHERE name=?",(name,)): return J({"error":"Room exists."},409)
    db_exec("INSERT INTO rooms(name,topic,created_by,created_at)VALUES(?,?,?,?)",(name,topic,u["display_name"],now()))
    await bcast({"type":"system","text":f"New room #{name} created!","time":ts(),"rooms":[r["name"] for r in rooms()]})
    return J({"ok":True,"room":name},201)

async def admin_ban(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    uid=req.match_info["uid"]
    if uid==str(u["id"]): return J({"error":"Cannot ban yourself."},400)
    d=await req.json()
    db_exec("UPDATE users SET is_banned=? WHERE id=?",(1 if d.get("banned") else 0,uid))
    return J({"ok":True})

async def admin_set_admin(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    d=await req.json(); uid=req.match_info["uid"]
    db_exec("UPDATE users SET is_admin=? WHERE id=?",(1 if d.get("is_admin") else 0,uid))
    return J({"ok":True})

async def admin_del_msg(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    db_exec("DELETE FROM messages WHERE id=?",(req.match_info["mid"],))
    return J({"ok":True})

async def admin_del_room(req):
    u=resolve(gtok(req))
    if not u or not u.get("is_admin"): return J({"error":"Admin only."},403)
    rn=req.match_info["rname"]
    if rn=="general": return J({"error":"Cannot delete #general."},400)
    db_exec("DELETE FROM rooms WHERE name=?",(rn,))
    return J({"ok":True})

async def health(req): return J({"status":"ok","app":"ThreadTalk v7"})

# ─── WebSocket handler ─────────────────────────────────────────────────────────
async def ws_route(req):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(req)
    print("[ws] + connection")

    pkt=None
    async for msg in ws:
        if msg.type==aiohttp.WSMsgType.TEXT:
            try:
                pkt=json.loads(msg.data)
                if pkt.get("type")=="join": break
            except: continue
        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
            return ws

    if not pkt: return ws

    token=pkt.get("token",""); room=pkt.get("room","general")
    user=resolve(token)
    if not user:
        await ws.send_str(json.dumps({"type":"error","text":"Invalid session."})); return ws
    if user.get("is_banned"):
        await ws.send_str(json.dumps({"type":"error","text":"Account banned."})); return ws

    name,clr,uid=user["display_name"],user["color"],user["id"]
    db_exec("UPDATE users SET last_seen=? WHERE id=?",(now(),uid))
    rlist=[r["name"] for r in rooms()]
    if room not in rlist: room="general"
    with ws_clients_lock: ws_clients[ws]={"name":name,"color":clr,"room":room,"user_id":uid}
    print(f"[ws] ★ {name} → #{room}")

    await ws.send_str(json.dumps({"type":"welcome","name":name,"color":clr,"room":room,
        "rooms":rlist,"history":history(room),"users":ulist(room),"stats":stats(),
        "leaderboard":lboard(),"time":ts(),"msg_count":user["msg_count"],
        "is_admin":bool(user["is_admin"]),"text":f"Welcome back, {name}!"}))
    await bcast({"type":"system","text":f"{name} joined #{room}.","time":ts(),
                 "users":ulist(room),"room":room,"stats":stats(),"leaderboard":lboard()},
                room=room, exclude=ws)

    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            try: pkt=json.loads(msg.data)
            except: continue
            mt=pkt.get("type")

            if mt=="message":
                text=pkt.get("text","").strip()
                if not text: continue
                cur=ws_clients.get(ws,{}).get("room","general")
                save_msg(cur,uid,name,text)
                payload={"type":"message","name":name,"color":clr,"text":text,
                         "time":ts(),"room":cur,"leaderboard":lboard(),"stats":stats()}
                await ws.send_str(json.dumps(payload))
                await bcast(payload,room=cur,exclude=ws)
                print(f"[ws] #{cur} {name}: {text}")

            elif mt=="switch_room":
                nr=pkt.get("room","general")
                if nr not in [r["name"] for r in rooms()]: continue
                old=ws_clients.get(ws,{}).get("room","general")
                with ws_clients_lock: ws_clients[ws]["room"]=nr
                await bcast({"type":"system","text":f"{name} left #{old}.","time":ts(),"users":ulist(old),"room":old},room=old)
                await ws.send_str(json.dumps({"type":"room_switched","room":nr,"history":history(nr),"users":ulist(nr),"time":ts()}))
                await bcast({"type":"system","text":f"{name} joined #{nr}.","time":ts(),"users":ulist(nr),"room":nr},room=nr,exclude=ws)

            elif mt=="typing":
                cur=ws_clients.get(ws,{}).get("room","general")
                await bcast({"type":"typing","name":name,"active":pkt.get("active",False),"room":cur},room=cur,exclude=ws)

            elif mt=="get_history":
                cur=ws_clients.get(ws,{}).get("room","general")
                await ws.send_str(json.dumps({"type":"history","room":cur,"messages":history(cur,100)}))

        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
            break

    with ws_clients_lock: info=ws_clients.pop(ws,None)
    if info:
        r=info.get("room","general"); print(f"[ws] - {info['name']} left #{r}")
        await bcast({"type":"system","text":f"{info['name']} left.","time":ts(),
                     "users":ulist(r),"room":r,"stats":stats()},room=r)
    return ws

# ─── App setup ─────────────────────────────────────────────────────────────────
def make_app():
    app = web.Application(middlewares=[cors])
    app.router.add_get("/",       health)
    app.router.add_get("/health", health)
    app.router.add_get("/ws",     ws_route)
    app.router.add_post("/api/auth/login",    login)
    app.router.add_post("/api/auth/register", register)
    app.router.add_post("/api/auth/logout",   logout)
    app.router.add_get( "/api/auth/me",       me)
    app.router.add_get( "/api/admin/stats",    admin_stats)
    app.router.add_get( "/api/admin/users",    admin_users)
    app.router.add_get( "/api/admin/messages", admin_messages)
    app.router.add_get( "/api/admin/rooms",    admin_rooms_get)
    app.router.add_post("/api/admin/rooms",    admin_rooms_post)
    app.router.add_put( "/api/admin/users/{uid}/ban",   admin_ban)
    app.router.add_put( "/api/admin/users/{uid}/admin", admin_set_admin)
    app.router.add_delete("/api/admin/messages/{mid}",  admin_del_msg)
    app.router.add_delete("/api/admin/rooms/{rname}",   admin_del_room)
    app.router.add_route("OPTIONS","/{path_info:.*}", lambda r: web.Response())
    return app

if __name__=="__main__":
    init_db()
    print(f"""
╔══════════════════════════════════════════════════╗
║  ThreadTalk v7  —  aiohttp + WebSocket           ║
║  Port   : {PORT}                                 ║
║  WS     : wss://threadtalks.onrender.com/ws      ║
║  API    : https://threadtalks.onrender.com/api/* ║
║  Admin  : admin / admin123                       ║
╚══════════════════════════════════════════════════╝
""")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)