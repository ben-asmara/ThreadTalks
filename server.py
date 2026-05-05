"""
=============================================================
  ThreadTalk v4  —  Cloud Edition
  Threads · Sockets · SQLite · Auth · WebSocket
=============================================================
  Single port handles EVERYTHING:
    GET  /health    → health check (Render needs this)
    *    /api/*     → REST API  (auth + admin)
    WS   /ws        → WebSocket chat  (one thread per client)

  Deploy to Render / Railway — zero extra config needed.
  Run locally:  python server.py
=============================================================
"""

import threading, sqlite3, json, sys, hashlib, secrets, os, struct, base64
import hashlib as hl
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Each HTTP/WebSocket request gets its own thread — required for Render."""
    daemon_threads = True

# ─── Config ───────────────────────────────────────────────────────────────────
PORT    = int(os.environ.get("PORT", 10000))
DB_FILE = os.environ.get("DB_FILE", "chat.db")

COLORS = ["#6c63ff","#00e5c3","#f59e0b","#ec4899","#3b82f6",
          "#10b981","#f97316","#a855f7","#06b6d4","#84cc16"]

# ─── Shared state ─────────────────────────────────────────────────────────────
ws_clients      = {}
ws_clients_lock = threading.Lock()
sessions        = {}
sessions_lock   = threading.Lock()

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
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        username      TEXT UNIQUE NOT NULL,
        display_name  TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        color         TEXT NOT NULL DEFAULT '#6c63ff',
        is_admin      INTEGER NOT NULL DEFAULT 0,
        is_banned     INTEGER NOT NULL DEFAULT 0,
        created_at    TEXT NOT NULL,
        last_seen     TEXT,
        msg_count     INTEGER DEFAULT 0
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS rooms (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        name       TEXT UNIQUE NOT NULL,
        topic      TEXT DEFAULT '',
        created_by TEXT DEFAULT 'system',
        created_at TEXT NOT NULL
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS messages (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        room      TEXT NOT NULL,
        user_id   INTEGER NOT NULL,
        user_name TEXT NOT NULL,
        text      TEXT NOT NULL,
        sent_at   TEXT NOT NULL
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS sessions (
        token      TEXT PRIMARY KEY,
        user_id    INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )""")
    for name, topic in [("general","General chat"),("random","Anything goes"),("tech","Tech talk")]:
        db_exec("INSERT OR IGNORE INTO rooms (name,topic,created_at) VALUES (?,?,?)",
                (name, topic, iso_now()))
    if not db_one("SELECT id FROM users WHERE username='admin'"):
        db_exec("""INSERT INTO users (username,display_name,password_hash,color,is_admin,created_at)
                   VALUES (?,?,?,?,1,?)""",
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
    with sessions_lock:
        sessions[token] = info
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

# ─── Pure-stdlib WebSocket (RFC 6455) ─────────────────────────────────────────
WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

def ws_handshake(rfile, wfile, headers):
    key    = headers.get("Sec-WebSocket-Key","").strip()
    accept = base64.b64encode(hl.sha1((key + WS_MAGIC).encode()).digest()).decode()
    wfile.write(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
        + f"Sec-WebSocket-Accept: {accept}\r\n\r\n".encode()
    )
    wfile.flush()

def ws_recv(rfile):
    try:
        b1, b2 = rfile.read(2)
    except Exception:
        return None, None
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", rfile.read(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", rfile.read(8))[0]
    mask = rfile.read(4) if masked else b""
    data = bytearray(rfile.read(length))
    if masked:
        data = bytearray(b ^ mask[i % 4] for i, b in enumerate(data))
    return opcode, bytes(data)

def ws_send(wfile, text: str):
    data   = text.encode("utf-8")
    length = len(data)
    hdr    = bytearray([0x81])
    if length < 126:       hdr.append(length)
    elif length < 65536:   hdr += bytearray([126]) + struct.pack("!H", length)
    else:                  hdr += bytearray([127]) + struct.pack("!Q", length)
    try:
        wfile.write(bytes(hdr) + data)
        wfile.flush()
        return True
    except Exception:
        return False

def ws_close(conn):
    try:
        conn["wfile"].write(bytes([0x88, 0x00]))
        conn["wfile"].flush()
    except: pass
    try: conn.get("sock") and conn["sock"].close()
    except: pass

# ─── Chat helpers ─────────────────────────────────────────────────────────────
def send_to(conn, payload):
    ws_send(conn["wfile"], json.dumps(payload))

def broadcast(payload, room=None, exclude=None):
    msg  = json.dumps(payload)
    dead = []
    with ws_clients_lock:
        for c, info in ws_clients.items():
            if c is exclude: continue
            if room and info.get("room") != room: continue
            if not ws_send(c["wfile"], msg): dead.append(c)
    for c in dead: remove_client(c)

def user_list(room=None):
    with ws_clients_lock:
        return [{"name":v["name"],"color":v["color"]}
                for v in ws_clients.values()
                if room is None or v.get("room")==room]

def remove_client(conn):
    with ws_clients_lock:
        info = ws_clients.pop(conn, None)
    if not info: return
    try: ws_close(conn)
    except: pass
    room = info.get("room","general")
    print(f"[ws] ✗ {info['name']} left #{room}")
    broadcast({"type":"system","text":f"{info['name']} left.","time":ts(),
               "users":user_list(room),"room":room,"stats":get_stats()}, room=room)

# ─── WebSocket client thread ──────────────────────────────────────────────────
def handle_ws(conn):
    tname = threading.current_thread().name

    # Wait for join packet
    while True:
        opcode, data = ws_recv(conn["rfile"])
        if opcode is None or opcode == 8: return
        if opcode != 1: continue
        try:
            pkt = json.loads(data.decode("utf-8","replace"))
            if pkt.get("type") == "join": break
        except: continue

    token = pkt.get("token","")
    room  = pkt.get("room","general")
    user  = resolve_session(token)

    if not user:
        send_to(conn, {"type":"error","text":"Invalid session. Please sign in."})
        ws_close(conn); return
    if user.get("is_banned"):
        send_to(conn, {"type":"error","text":"Your account has been banned."})
        ws_close(conn); return

    name    = user["display_name"]
    color   = user["color"]
    user_id = user["id"]
    db_exec("UPDATE users SET last_seen=? WHERE id=?", (iso_now(), user_id))

    rooms_list = [r["name"] for r in get_rooms()]
    if room not in rooms_list: room = "general"

    with ws_clients_lock:
        ws_clients[conn] = {"name":name,"color":color,"room":room,"user_id":user_id}

    print(f"[ws] ★  {name} → #{room}  [{tname}]")

    send_to(conn, {
        "type":"welcome","name":name,"color":color,"room":room,
        "rooms":rooms_list,"history":get_history(room),
        "users":user_list(room),"stats":get_stats(),
        "leaderboard":get_leaderboard(),"time":ts(),
        "msg_count":user["msg_count"],"is_admin":bool(user["is_admin"]),
        "text":f"Welcome back, {name}!",
    })
    broadcast({"type":"system","text":f"{name} joined #{room}.","time":ts(),
               "users":user_list(room),"room":room,"stats":get_stats(),
               "leaderboard":get_leaderboard()}, room=room, exclude=conn)

    while True:
        opcode, data = ws_recv(conn["rfile"])
        if opcode is None or opcode == 8: break
        if opcode != 1: continue
        try: pkt = json.loads(data.decode("utf-8","replace"))
        except: continue

        mtype = pkt.get("type")

        if mtype == "message":
            text = pkt.get("text","").strip()
            if not text: continue
            cur = ws_clients.get(conn,{}).get("room","general")
            save_message(cur, user_id, name, text)
            payload = {"type":"message","name":name,"color":color,"text":text,
                       "time":ts(),"room":cur,
                       "leaderboard":get_leaderboard(),"stats":get_stats()}
            send_to(conn, payload)
            broadcast(payload, room=cur, exclude=conn)

        elif mtype == "switch_room":
            nr = pkt.get("room","general")
            if nr not in [r["name"] for r in get_rooms()]: continue
            old = ws_clients[conn].get("room","general")
            with ws_clients_lock: ws_clients[conn]["room"] = nr
            broadcast({"type":"system","text":f"{name} left #{old}.","time":ts(),
                       "users":user_list(old),"room":old}, room=old)
            send_to(conn, {"type":"room_switched","room":nr,
                           "history":get_history(nr),"users":user_list(nr),"time":ts()})
            broadcast({"type":"system","text":f"{name} joined #{nr}.","time":ts(),
                       "users":user_list(nr),"room":nr}, room=nr, exclude=conn)

        elif mtype == "typing":
            cur = ws_clients.get(conn,{}).get("room","general")
            broadcast({"type":"typing","name":name,"active":pkt.get("active",False),
                       "room":cur}, room=cur, exclude=conn)

        elif mtype == "get_history":
            cur = ws_clients.get(conn,{}).get("room","general")
            send_to(conn,{"type":"history","room":cur,"messages":get_history(cur,100)})

    remove_client(conn)

# ─── Unified HTTP + WebSocket request handler ─────────────────────────────────
def json_resp(h, code, data):
    body = json.dumps(data).encode()
    h.send_response(code)
    h.send_header("Content-Type","application/json")
    h.send_header("Content-Length", len(body))
    h.send_header("Access-Control-Allow-Origin","*")
    h.send_header("Access-Control-Allow-Headers","Content-Type,Authorization")
    h.send_header("Access-Control-Allow-Methods","GET,POST,PUT,DELETE,OPTIONS")
    h.end_headers()
    h.wfile.write(body)

def get_token(h):
    return h.headers.get("Authorization","").replace("Bearer ","").strip()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Headers","Content-Type,Authorization")
        self.send_header("Access-Control-Allow-Methods","GET,POST,PUT,DELETE,OPTIONS")
        self.end_headers()

    def read_body(self):
        n = int(self.headers.get("Content-Length",0))
        return json.loads(self.rfile.read(n)) if n else {}

    def do_GET(self):
        # WebSocket upgrade
        if self.headers.get("Upgrade","").lower() == "websocket":
            # Use raw socket with unbuffered files for reliable WS framing
            raw = self.connection
            rfile = raw.makefile("rb", buffering=0)
            wfile = raw.makefile("wb", buffering=0)
            ws_handshake(rfile, wfile, self.headers)
            conn = {"rfile": rfile, "wfile": wfile, "sock": raw}
            # handle_ws runs in THIS thread (ThreadingMixIn gives us our own thread)
            # so the socket stays open for the lifetime of the connection
            handle_ws(conn)
            return

        path = urlparse(self.path).path

        if path in ("/","/health"):
            body = b'{"status":"ok","app":"ThreadTalk v4"}'
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",len(body))
            self.end_headers(); self.wfile.write(body)

        elif path == "/api/auth/me":
            u = resolve_session(get_token(self))
            if not u: return json_resp(self,401,{"error":"Not authenticated."})
            json_resp(self,200,{"user":{"id":u["id"],"username":u["username"],
                "display_name":u["display_name"],"color":u["color"],
                "is_admin":bool(u["is_admin"]),"msg_count":u["msg_count"]}})

        elif path == "/api/admin/users":
            u = resolve_session(get_token(self))
            if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
            rows = db_query("SELECT id,username,display_name,color,is_admin,is_banned,created_at,last_seen,msg_count FROM users ORDER BY created_at DESC")
            json_resp(self,200,{"users":[dict(r) for r in rows]})

        elif path == "/api/admin/messages":
            u = resolve_session(get_token(self))
            if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
            rows = db_query("SELECT id,room,user_name,text,sent_at FROM messages ORDER BY id DESC LIMIT 100")
            json_resp(self,200,{"messages":[dict(r) for r in rows]})

        elif path == "/api/admin/rooms":
            u = resolve_session(get_token(self))
            if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
            rows = db_query("SELECT r.*,(SELECT COUNT(*) FROM messages m WHERE m.room=r.name) as msg_count FROM rooms r ORDER BY r.name")
            json_resp(self,200,{"rooms":[dict(r) for r in rows]})

        elif path == "/api/admin/stats":
            u = resolve_session(get_token(self))
            if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
            stats = get_stats(); stats["online"] = len(ws_clients)
            json_resp(self,200,{"stats":stats,"leaderboard":get_leaderboard()})

        else:
            json_resp(self,404,{"error":"Not found."})

    def do_POST(self):
        path = urlparse(self.path).path

        if path == "/api/auth/register":
            d = self.read_body()
            username = (d.get("username") or "").strip().lower()[:30]
            display  = (d.get("display_name") or username).strip()[:30]
            password = d.get("password","")
            if not username or not password:
                return json_resp(self,400,{"error":"Username and password required."})
            if len(password) < 6:
                return json_resp(self,400,{"error":"Password must be at least 6 characters."})
            if db_one("SELECT id FROM users WHERE username=?", (username,)):
                return json_resp(self,409,{"error":"Username already taken."})
            color = pick_color()
            db_exec("INSERT INTO users (username,display_name,password_hash,color,created_at) VALUES (?,?,?,?,?)",
                    (username,display,hash_pw(password),color,iso_now()))
            user  = dict(db_one("SELECT * FROM users WHERE username=?", (username,)))
            token = new_token()
            db_exec("INSERT INTO sessions (token,user_id,created_at) VALUES (?,?,?)",(token,user["id"],iso_now()))
            print(f"[auth] New user: {display}")
            json_resp(self,201,{"token":token,"user":{"id":user["id"],"username":username,
                "display_name":display,"color":color,"is_admin":False}})

        elif path == "/api/auth/login":
            d = self.read_body()
            username = (d.get("username") or "").strip().lower()
            password = d.get("password","")
            user = db_one("SELECT * FROM users WHERE username=? AND password_hash=?",
                          (username,hash_pw(password)))
            if not user: return json_resp(self,401,{"error":"Invalid username or password."})
            user = dict(user)
            if user.get("is_banned"): return json_resp(self,403,{"error":"Account banned."})
            token = new_token()
            db_exec("INSERT INTO sessions (token,user_id,created_at) VALUES (?,?,?)",(token,user["id"],iso_now()))
            db_exec("UPDATE users SET last_seen=? WHERE id=?",(iso_now(),user["id"]))
            with sessions_lock: sessions[token] = user
            print(f"[auth] Login: {user['display_name']}")
            json_resp(self,200,{"token":token,"user":{"id":user["id"],"username":user["username"],
                "display_name":user["display_name"],"color":user["color"],"is_admin":bool(user["is_admin"])}})

        elif path == "/api/auth/logout":
            token = get_token(self)
            db_exec("DELETE FROM sessions WHERE token=?",(token,))
            with sessions_lock: sessions.pop(token,None)
            json_resp(self,200,{"ok":True})

        elif path == "/api/admin/rooms":
            u = resolve_session(get_token(self))
            if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
            d    = self.read_body()
            name  = (d.get("name") or "").strip().lower().replace(" ","-")[:20]
            topic = (d.get("topic") or "").strip()[:80]
            if not name: return json_resp(self,400,{"error":"Room name required."})
            if db_one("SELECT id FROM rooms WHERE name=?",(name,)):
                return json_resp(self,409,{"error":"Room already exists."})
            db_exec("INSERT INTO rooms (name,topic,created_by,created_at) VALUES (?,?,?,?)",
                    (name,topic,u["display_name"],iso_now()))
            broadcast({"type":"system","text":f"New room #{name} created!","time":ts(),
                       "rooms":[r["name"] for r in get_rooms()]})
            json_resp(self,201,{"ok":True,"room":name})

        else:
            json_resp(self,404,{"error":"Not found."})

    def do_PUT(self):
        path = urlparse(self.path).path
        u = resolve_session(get_token(self))
        if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
        d = self.read_body()
        if path.startswith("/api/admin/users/") and path.endswith("/ban"):
            uid = path.split("/")[4]
            if uid == str(u["id"]): return json_resp(self,400,{"error":"Cannot ban yourself."})
            db_exec("UPDATE users SET is_banned=? WHERE id=?",(1 if d.get("banned") else 0, uid))
            json_resp(self,200,{"ok":True})
        elif path.startswith("/api/admin/users/") and path.endswith("/admin"):
            uid = path.split("/")[4]
            db_exec("UPDATE users SET is_admin=? WHERE id=?",(1 if d.get("is_admin") else 0, uid))
            json_resp(self,200,{"ok":True})
        else:
            json_resp(self,404,{"error":"Not found."})

    def do_DELETE(self):
        path = urlparse(self.path).path
        u = resolve_session(get_token(self))
        if not u or not u.get("is_admin"): return json_resp(self,403,{"error":"Admin only."})
        if path.startswith("/api/admin/messages/"):
            db_exec("DELETE FROM messages WHERE id=?",(path.split("/")[4],))
            json_resp(self,200,{"ok":True})
        elif path.startswith("/api/admin/rooms/"):
            rname = path.split("/")[4]
            if rname == "general": return json_resp(self,400,{"error":"Cannot delete #general."})
            db_exec("DELETE FROM rooms WHERE name=?",(rname,))
            json_resp(self,200,{"ok":True})
        else:
            json_resp(self,404,{"error":"Not found."})

# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    print(f"""
╔══════════════════════════════════════════════════╗
║  ThreadTalk v4  —  Cloud Edition                 ║
║  Port     : {PORT}                               ║
║  WebSocket: ws://0.0.0.0:{PORT}/ws               ║
║  REST API : http://0.0.0.0:{PORT}/api/*          ║
║  Admin    : admin / admin123                     ║
╚══════════════════════════════════════════════════╝
""")
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] Listening on :{PORT} …  (Ctrl-C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[server] Bye.")
        sys.exit(0)