"""
=============================================================
  ThreadTalk v5  —  Render-Ready
  Threads · Sockets · SQLite · Auth · websockets lib
=============================================================
  Two servers, one process:
    Thread 1  →  HTTP  :PORT      REST API
    Thread 2  →  WS    :PORT+1    WebSocket chat

  In Render:
    - HTTP_PORT = $PORT  (public, what Render exposes)
    - WS_PORT   = $PORT+1 (also needs to be exposed — see below)

  IMPORTANT: In Render dashboard → your service → Settings
  Add a second port:  PORT+1  (e.g. if PORT=10000, add 10001)
  Then in chat.html use:
    const WS_URL = API.replace(/^http/, "ws")
                      .replace(/:\d+/, ":" + (parseInt(...)+1)) + "/ws"
  OR just hardcode the WS URL as shown in chat.html.
=============================================================
"""

import asyncio, threading, sqlite3, json, sys, hashlib, secrets, os
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse
import websockets

# ─── Config ───────────────────────────────────────────────────────────────────
HTTP_PORT = int(os.environ.get("PORT", 10000))
WS_PORT   = int(os.environ.get("WS_PORT", HTTP_PORT + 1))
DB_FILE   = os.environ.get("DB_FILE", "chat.db")

COLORS = ["#6c63ff","#00e5c3","#f59e0b","#ec4899","#3b82f6",
          "#10b981","#f97316","#a855f7","#06b6d4","#84cc16"]

# ─── Shared state ─────────────────────────────────────────────────────────────
ws_clients      = {}
ws_clients_lock = threading.Lock()
sessions        = {}
sessions_lock   = threading.Lock()
_ws_loop        = None   # asyncio loop for WS thread

# ─── Database ─────────────────────────────────────────────────────────────────
_db_lock = threading.Lock()
_db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)
_db_conn.row_factory = sqlite3.Row

def db_exec(sql, params=()):
    with _db_lock:
        cur = _db_conn.execute(sql, params)
        _db_conn.commit()
        return cur

def db_query(sql, params=()):
    with _db_lock:
        return _db_conn.execute(sql, params).fetchall()

def db_one(sql, params=()):
    with _db_lock:
        return _db_conn.execute(sql, params).fetchone()

def init_db():
    db_exec("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        color TEXT NOT NULL DEFAULT '#6c63ff',
        is_admin INTEGER NOT NULL DEFAULT 0,
        is_banned INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        last_seen TEXT,
        msg_count INTEGER DEFAULT 0
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS rooms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        topic TEXT DEFAULT '',
        created_by TEXT DEFAULT 'system',
        created_at TEXT NOT NULL
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        room TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        user_name TEXT NOT NULL,
        text TEXT NOT NULL,
        sent_at TEXT NOT NULL
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )""")
    for name, topic in [("general","General chat"),("random","Anything goes"),("tech","Tech talk")]:
        db_exec("INSERT OR IGNORE INTO rooms (name,topic,created_at) VALUES (?,?,?)",
                (name, topic, iso_now()))
    if not db_one("SELECT id FROM users WHERE username='admin'"):
        db_exec("INSERT INTO users (username,display_name,password_hash,color,is_admin,created_at) VALUES (?,?,?,?,1,?)",
                ("admin","Administrator",hash_pw("admin123"),"#6c63ff",iso_now()))
        print("[db] Admin created → admin / admin123")
    print(f"[db] SQLite ready ✓ → {DB_FILE}")

# ─── Helpers ──────────────────────────────────────────────────────────────────
def iso_now():   return datetime.now().isoformat(timespec="seconds")
def ts():        return datetime.now().strftime("%H:%M")
def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def new_token(): return secrets.token_hex(32)

def pick_color():
    n = db_query("SELECT COUNT(*) as n FROM users")[0]["n"]
    return COLORS[n % len(COLORS)]

def resolve_session(token):
    if not token: return None
    with sessions_lock:
        if token in sessions: return sessions[token]
    row = db_one("SELECT u.* FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?", (token,))
    if not row: return None
    info = dict(row)
    with sessions_lock: sessions[token] = info
    return info

def get_history(room, limit=40):
    rows = db_query(
        "SELECT m.user_name,m.text,m.sent_at,u.color FROM messages m "
        "LEFT JOIN users u ON m.user_id=u.id "
        "WHERE m.room=? ORDER BY m.id DESC LIMIT ?", (room, limit))
    return list(reversed([dict(r) for r in rows]))

def get_rooms():
    return [dict(r) for r in db_query("SELECT name,topic FROM rooms ORDER BY name")]

def get_leaderboard():
    rows = db_query("SELECT display_name as name,color,msg_count FROM users ORDER BY msg_count DESC LIMIT 10")
    return [dict(r) for r in rows]

def get_stats():
    return {
        "messages": db_one("SELECT COUNT(*) as c FROM messages")["c"],
        "users":    db_one("SELECT COUNT(*) as c FROM users")["c"],
        "rooms":    db_one("SELECT COUNT(*) as c FROM rooms")["c"],
    }

def save_message(room, user_id, user_name, text):
    db_exec("INSERT INTO messages (room,user_id,user_name,text,sent_at) VALUES (?,?,?,?,?)",
            (room, user_id, user_name, text, iso_now()))
    db_exec("UPDATE users SET msg_count=msg_count+1,last_seen=? WHERE id=?", (iso_now(), user_id))

# ─── WS helpers ───────────────────────────────────────────────────────────────
def user_list(room=None):
    with ws_clients_lock:
        return [{"name":v["name"],"color":v["color"]}
                for v in ws_clients.values()
                if room is None or v.get("room")==room]

async def broadcast(payload, room=None, exclude=None):
    msg = json.dumps(payload)
    with ws_clients_lock:
        targets = [ws for ws, info in ws_clients.items()
                   if ws is not exclude
                   and (room is None or info.get("room")==room)]
    dead = []
    for ws in targets:
        try: await ws.send(msg)
        except: dead.append(ws)
    for ws in dead:
        with ws_clients_lock: ws_clients.pop(ws, None)

def broadcast_from_thread(payload, room=None):
    """Called from HTTP thread → schedules coroutine on WS loop."""
    if _ws_loop:
        asyncio.run_coroutine_threadsafe(broadcast(payload, room), _ws_loop)

# ─── WebSocket chat handler ────────────────────────────────────────────────────
async def ws_handler(websocket):
    print(f"[ws] + connection")

    # Handshake — wait for join packet
    pkt = None
    try:
        async for raw in websocket:
            try:
                pkt = json.loads(raw)
                if pkt.get("type") == "join": break
            except: continue
    except Exception: return
    if not pkt: return

    token = pkt.get("token","")
    room  = pkt.get("room","general")
    user  = resolve_session(token)

    if not user:
        await websocket.send(json.dumps({"type":"error","text":"Invalid session. Please sign in."}))
        return
    if user.get("is_banned"):
        await websocket.send(json.dumps({"type":"error","text":"Account banned."}))
        return

    name, color, user_id = user["display_name"], user["color"], user["id"]
    db_exec("UPDATE users SET last_seen=? WHERE id=?", (iso_now(), user_id))

    rooms_list = [r["name"] for r in get_rooms()]
    if room not in rooms_list: room = "general"

    with ws_clients_lock:
        ws_clients[websocket] = {"name":name,"color":color,"room":room,"user_id":user_id}

    print(f"[ws] ★ {name} → #{room}")

    await websocket.send(json.dumps({
        "type":"welcome","name":name,"color":color,"room":room,
        "rooms":rooms_list,"history":get_history(room),
        "users":user_list(room),"stats":get_stats(),
        "leaderboard":get_leaderboard(),"time":ts(),
        "msg_count":user["msg_count"],"is_admin":bool(user["is_admin"]),
        "text":f"Welcome back, {name}!",
    }))
    await broadcast({"type":"system","text":f"{name} joined #{room}.","time":ts(),
                     "users":user_list(room),"room":room,"stats":get_stats(),
                     "leaderboard":get_leaderboard()}, room=room, exclude=websocket)

    try:
        async for raw in websocket:
            try: pkt = json.loads(raw)
            except: continue
            mtype = pkt.get("type")

            if mtype == "message":
                text = pkt.get("text","").strip()
                if not text: continue
                cur = ws_clients.get(websocket,{}).get("room","general")
                save_message(cur, user_id, name, text)
                payload = {"type":"message","name":name,"color":color,"text":text,
                           "time":ts(),"room":cur,
                           "leaderboard":get_leaderboard(),"stats":get_stats()}
                await websocket.send(json.dumps(payload))
                await broadcast(payload, room=cur, exclude=websocket)
                print(f"[ws] #{cur} {name}: {text}")

            elif mtype == "switch_room":
                nr = pkt.get("room","general")
                if nr not in [r["name"] for r in get_rooms()]: continue
                old = ws_clients.get(websocket,{}).get("room","general")
                with ws_clients_lock: ws_clients[websocket]["room"] = nr
                await broadcast({"type":"system","text":f"{name} left #{old}.","time":ts(),
                                  "users":user_list(old),"room":old}, room=old)
                await websocket.send(json.dumps({"type":"room_switched","room":nr,
                    "history":get_history(nr),"users":user_list(nr),"time":ts()}))
                await broadcast({"type":"system","text":f"{name} joined #{nr}.","time":ts(),
                                  "users":user_list(nr),"room":nr}, room=nr, exclude=websocket)

            elif mtype == "typing":
                cur = ws_clients.get(websocket,{}).get("room","general")
                await broadcast({"type":"typing","name":name,"active":pkt.get("active",False),
                                  "room":cur}, room=cur, exclude=websocket)

            elif mtype == "get_history":
                cur = ws_clients.get(websocket,{}).get("room","general")
                await websocket.send(json.dumps({"type":"history","room":cur,
                                                  "messages":get_history(cur,100)}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        with ws_clients_lock:
            info = ws_clients.pop(websocket, None)
        if info:
            r = info.get("room","general")
            print(f"[ws] - {info['name']} left #{r}")
            await broadcast({"type":"system","text":f"{info['name']} left.","time":ts(),
                              "users":user_list(r),"room":r,"stats":get_stats()}, room=r)

# ─── WS event-loop thread ──────────────────────────────────────────────────────
def run_ws():
    global _ws_loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _ws_loop = loop

    async def _main():
        print(f"[ws]  Listening on :{WS_PORT}")
        async with websockets.serve(ws_handler, "0.0.0.0", WS_PORT,
                                    ping_interval=30, ping_timeout=10):
            await asyncio.Future()

    loop.run_until_complete(_main())

# ─── HTTP REST API ─────────────────────────────────────────────────────────────
class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

def jresp(h, code, data):
    body = json.dumps(data).encode()
    h.send_response(code)
    h.send_header("Content-Type","application/json")
    h.send_header("Content-Length",len(body))
    h.send_header("Access-Control-Allow-Origin","*")
    h.send_header("Access-Control-Allow-Headers","Content-Type,Authorization")
    h.send_header("Access-Control-Allow-Methods","GET,POST,PUT,DELETE,OPTIONS")
    h.end_headers(); h.wfile.write(body)

def gtok(h): return h.headers.get("Authorization","").replace("Bearer ","").strip()

class H(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Headers","Content-Type,Authorization")
        self.send_header("Access-Control-Allow-Methods","GET,POST,PUT,DELETE,OPTIONS")
        self.end_headers()
    def rbody(self):
        n=int(self.headers.get("Content-Length",0))
        return json.loads(self.rfile.read(n)) if n else {}

    def do_GET(self):
        p=urlparse(self.path).path
        if p in ("/","/health"):
            b=b'{"status":"ok","app":"ThreadTalk v5"}'
            self.send_response(200); self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(b)); self.end_headers(); self.wfile.write(b)
        elif p=="/api/auth/me":
            u=resolve_session(gtok(self))
            if not u: return jresp(self,401,{"error":"Not authenticated."})
            jresp(self,200,{"user":{"id":u["id"],"username":u["username"],"display_name":u["display_name"],
                "color":u["color"],"is_admin":bool(u["is_admin"]),"msg_count":u["msg_count"]}})
        elif p=="/api/admin/users":
            u=resolve_session(gtok(self))
            if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
            jresp(self,200,{"users":[dict(r) for r in db_query("SELECT id,username,display_name,color,is_admin,is_banned,created_at,last_seen,msg_count FROM users ORDER BY created_at DESC")]})
        elif p=="/api/admin/messages":
            u=resolve_session(gtok(self))
            if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
            jresp(self,200,{"messages":[dict(r) for r in db_query("SELECT id,room,user_name,text,sent_at FROM messages ORDER BY id DESC LIMIT 100")]})
        elif p=="/api/admin/rooms":
            u=resolve_session(gtok(self))
            if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
            jresp(self,200,{"rooms":[dict(r) for r in db_query("SELECT r.*,(SELECT COUNT(*) FROM messages m WHERE m.room=r.name) as msg_count FROM rooms r ORDER BY r.name")]})
        elif p=="/api/admin/stats":
            u=resolve_session(gtok(self))
            if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
            s=get_stats(); s["online"]=len(ws_clients)
            jresp(self,200,{"stats":s,"leaderboard":get_leaderboard()})
        else: jresp(self,404,{"error":"Not found."})

    def do_POST(self):
        p=urlparse(self.path).path
        if p=="/api/auth/register":
            d=self.rbody(); username=(d.get("username") or "").strip().lower()[:30]
            display=(d.get("display_name") or username).strip()[:30]; pw=d.get("password","")
            if not username or not pw: return jresp(self,400,{"error":"Username and password required."})
            if len(pw)<6: return jresp(self,400,{"error":"Password min 6 chars."})
            if db_one("SELECT id FROM users WHERE username=?",(username,)):
                return jresp(self,409,{"error":"Username taken."})
            color=pick_color()
            db_exec("INSERT INTO users (username,display_name,password_hash,color,created_at) VALUES (?,?,?,?,?)",
                    (username,display,hash_pw(pw),color,iso_now()))
            user=dict(db_one("SELECT * FROM users WHERE username=?",(username,)))
            token=new_token()
            db_exec("INSERT INTO sessions (token,user_id,created_at) VALUES (?,?,?)",(token,user["id"],iso_now()))
            print(f"[auth] + {display}")
            jresp(self,201,{"token":token,"user":{"id":user["id"],"username":username,
                "display_name":display,"color":color,"is_admin":False}})
        elif p=="/api/auth/login":
            d=self.rbody(); username=(d.get("username") or "").strip().lower(); pw=d.get("password","")
            user=db_one("SELECT * FROM users WHERE username=? AND password_hash=?",(username,hash_pw(pw)))
            if not user: return jresp(self,401,{"error":"Invalid username or password."})
            user=dict(user)
            if user.get("is_banned"): return jresp(self,403,{"error":"Account banned."})
            token=new_token()
            db_exec("INSERT INTO sessions (token,user_id,created_at) VALUES (?,?,?)",(token,user["id"],iso_now()))
            db_exec("UPDATE users SET last_seen=? WHERE id=?",(iso_now(),user["id"]))
            with sessions_lock: sessions[token]=user
            print(f"[auth] → {user['display_name']}")
            jresp(self,200,{"token":token,"user":{"id":user["id"],"username":user["username"],
                "display_name":user["display_name"],"color":user["color"],"is_admin":bool(user["is_admin"])}})
        elif p=="/api/auth/logout":
            token=gtok(self); db_exec("DELETE FROM sessions WHERE token=?",(token,))
            with sessions_lock: sessions.pop(token,None)
            jresp(self,200,{"ok":True})
        elif p=="/api/admin/rooms":
            u=resolve_session(gtok(self))
            if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
            d=self.rbody(); name=(d.get("name") or "").strip().lower().replace(" ","-")[:20]
            topic=(d.get("topic") or "").strip()[:80]
            if not name: return jresp(self,400,{"error":"Room name required."})
            if db_one("SELECT id FROM rooms WHERE name=?",(name,)): return jresp(self,409,{"error":"Room exists."})
            db_exec("INSERT INTO rooms (name,topic,created_by,created_at) VALUES (?,?,?,?)",
                    (name,topic,u["display_name"],iso_now()))
            broadcast_from_thread({"type":"system","text":f"New room #{name} created!","time":ts(),
                                   "rooms":[r["name"] for r in get_rooms()]})
            jresp(self,201,{"ok":True,"room":name})
        else: jresp(self,404,{"error":"Not found."})

    def do_PUT(self):
        p=urlparse(self.path).path; u=resolve_session(gtok(self))
        if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
        d=self.rbody()
        if p.startswith("/api/admin/users/") and p.endswith("/ban"):
            uid=p.split("/")[4]
            if uid==str(u["id"]): return jresp(self,400,{"error":"Cannot ban yourself."})
            db_exec("UPDATE users SET is_banned=? WHERE id=?",(1 if d.get("banned") else 0,uid))
            jresp(self,200,{"ok":True})
        elif p.startswith("/api/admin/users/") and p.endswith("/admin"):
            db_exec("UPDATE users SET is_admin=? WHERE id=?",(1 if d.get("is_admin") else 0,p.split("/")[4]))
            jresp(self,200,{"ok":True})
        else: jresp(self,404,{"error":"Not found."})

    def do_DELETE(self):
        p=urlparse(self.path).path; u=resolve_session(gtok(self))
        if not u or not u.get("is_admin"): return jresp(self,403,{"error":"Admin only."})
        if p.startswith("/api/admin/messages/"):
            db_exec("DELETE FROM messages WHERE id=?",(p.split("/")[4],)); jresp(self,200,{"ok":True})
        elif p.startswith("/api/admin/rooms/"):
            rn=p.split("/")[4]
            if rn=="general": return jresp(self,400,{"error":"Cannot delete #general."})
            db_exec("DELETE FROM rooms WHERE name=?",(rn,)); jresp(self,200,{"ok":True})
        else: jresp(self,404,{"error":"Not found."})

# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__=="__main__":
    init_db()
    print(f"""
╔═══════════════════════════════════════════════╗
║  ThreadTalk v5                                ║
║  HTTP  : 0.0.0.0:{HTTP_PORT}  (REST API)      ║
║  WS    : 0.0.0.0:{WS_PORT}  (WebSocket)       ║
║  Admin : admin / admin123                     ║
╚═══════════════════════════════════════════════╝
""")
    threading.Thread(target=run_ws, daemon=True, name="ws").start()
    srv=ThreadingHTTPServer(("0.0.0.0",HTTP_PORT),H)
    print(f"[http] Listening on :{HTTP_PORT}")
    try: srv.serve_forever()
    except KeyboardInterrupt: print("\nBye."); sys.exit(0)
    