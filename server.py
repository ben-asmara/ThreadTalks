"""
=============================================================
  ThreadTalk v6  —  Single Port (Render Free Tier)
  Threads · Sockets · SQLite · Auth · WebSocket
=============================================================
  Everything on ONE port using websockets.serve() with
  process_request to handle HTTP REST calls directly.

  WS  →  GET /ws  (Upgrade: websocket)
  API →  any other path  (regular HTTP)

  Run:   python server.py
  Port:  $PORT env var (Render sets this automatically)
=============================================================
"""

import asyncio, threading, sqlite3, json, sys, hashlib, secrets, os
from datetime import datetime
from urllib.parse import urlparse
from http import HTTPStatus
import websockets
from websockets.server import serve
from websockets.http11 import Request

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
        db_exec("""INSERT INTO users
                   (username,display_name,password_hash,color,is_admin,created_at)
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
    row = db_one(
        "SELECT u.* FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?",
        (token,))
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
    rows = db_query(
        "SELECT display_name as name,color,msg_count FROM users "
        "ORDER BY msg_count DESC LIMIT 10")
    return [dict(r) for r in rows]

def get_stats():
    return {
        "messages": db_one("SELECT COUNT(*) as c FROM messages")["c"],
        "users":    db_one("SELECT COUNT(*) as c FROM users")["c"],
        "rooms":    db_one("SELECT COUNT(*) as c FROM rooms")["c"],
    }

def save_message(room, user_id, user_name, text):
    db_exec(
        "INSERT INTO messages (room,user_id,user_name,text,sent_at) VALUES (?,?,?,?,?)",
        (room, user_id, user_name, text, iso_now()))
    db_exec("UPDATE users SET msg_count=msg_count+1,last_seen=? WHERE id=?",
            (iso_now(), user_id))

# ─── WebSocket helpers ────────────────────────────────────────────────────────
def user_list(room=None):
    with ws_clients_lock:
        return [{"name": v["name"], "color": v["color"]}
                for v in ws_clients.values()
                if room is None or v.get("room") == room]

async def broadcast(payload, room=None, exclude=None):
    msg = json.dumps(payload)
    with ws_clients_lock:
        targets = [ws for ws, info in ws_clients.items()
                   if ws is not exclude
                   and (room is None or info.get("room") == room)]
    dead = []
    for ws in targets:
        try:
            await ws.send(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        with ws_clients_lock:
            ws_clients.pop(ws, None)

# ─── WebSocket chat handler ───────────────────────────────────────────────────
async def ws_handler(websocket):
    print(f"[ws] + new connection")

    # Wait for join packet
    pkt = None
    try:
        async for raw in websocket:
            try:
                pkt = json.loads(raw)
                if pkt.get("type") == "join":
                    break
            except Exception:
                continue
    except Exception:
        return
    if not pkt:
        return

    token = pkt.get("token", "")
    room  = pkt.get("room", "general")
    user  = resolve_session(token)

    if not user:
        await websocket.send(json.dumps(
            {"type": "error", "text": "Invalid session. Please sign in."}))
        return
    if user.get("is_banned"):
        await websocket.send(json.dumps(
            {"type": "error", "text": "Your account has been banned."}))
        return

    name    = user["display_name"]
    color   = user["color"]
    user_id = user["id"]
    db_exec("UPDATE users SET last_seen=? WHERE id=?", (iso_now(), user_id))

    rooms_list = [r["name"] for r in get_rooms()]
    if room not in rooms_list:
        room = "general"

    with ws_clients_lock:
        ws_clients[websocket] = {
            "name": name, "color": color, "room": room, "user_id": user_id}

    print(f"[ws] ★ {name} → #{room}")

    await websocket.send(json.dumps({
        "type": "welcome", "name": name, "color": color, "room": room,
        "rooms": rooms_list, "history": get_history(room),
        "users": user_list(room), "stats": get_stats(),
        "leaderboard": get_leaderboard(), "time": ts(),
        "msg_count": user["msg_count"], "is_admin": bool(user["is_admin"]),
        "text": f"Welcome back, {name}!",
    }))

    await broadcast(
        {"type": "system", "text": f"{name} joined #{room}.", "time": ts(),
         "users": user_list(room), "room": room, "stats": get_stats(),
         "leaderboard": get_leaderboard()},
        room=room, exclude=websocket)

    try:
        async for raw in websocket:
            try:
                pkt = json.loads(raw)
            except Exception:
                continue
            mtype = pkt.get("type")

            if mtype == "message":
                text = pkt.get("text", "").strip()
                if not text:
                    continue
                cur = ws_clients.get(websocket, {}).get("room", "general")
                save_message(cur, user_id, name, text)
                payload = {
                    "type": "message", "name": name, "color": color,
                    "text": text, "time": ts(), "room": cur,
                    "leaderboard": get_leaderboard(), "stats": get_stats()}
                await websocket.send(json.dumps(payload))
                await broadcast(payload, room=cur, exclude=websocket)
                print(f"[ws] #{cur} {name}: {text}")

            elif mtype == "switch_room":
                nr = pkt.get("room", "general")
                if nr not in [r["name"] for r in get_rooms()]:
                    continue
                old = ws_clients.get(websocket, {}).get("room", "general")
                with ws_clients_lock:
                    ws_clients[websocket]["room"] = nr
                await broadcast(
                    {"type": "system", "text": f"{name} left #{old}.",
                     "time": ts(), "users": user_list(old), "room": old},
                    room=old)
                await websocket.send(json.dumps(
                    {"type": "room_switched", "room": nr,
                     "history": get_history(nr),
                     "users": user_list(nr), "time": ts()}))
                await broadcast(
                    {"type": "system", "text": f"{name} joined #{nr}.",
                     "time": ts(), "users": user_list(nr), "room": nr},
                    room=nr, exclude=websocket)

            elif mtype == "typing":
                cur = ws_clients.get(websocket, {}).get("room", "general")
                await broadcast(
                    {"type": "typing", "name": name,
                     "active": pkt.get("active", False), "room": cur},
                    room=cur, exclude=websocket)

            elif mtype == "get_history":
                cur = ws_clients.get(websocket, {}).get("room", "general")
                await websocket.send(json.dumps(
                    {"type": "history", "room": cur,
                     "messages": get_history(cur, 100)}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        with ws_clients_lock:
            info = ws_clients.pop(websocket, None)
        if info:
            r = info.get("room", "general")
            print(f"[ws] - {info['name']} left #{r}")
            await broadcast(
                {"type": "system", "text": f"{info['name']} left.",
                 "time": ts(), "users": user_list(r), "room": r,
                 "stats": get_stats()},
                room=r)

# ─── HTTP REST handler (called from process_request hook) ─────────────────────
CORS = [
    ("Access-Control-Allow-Origin",  "*"),
    ("Access-Control-Allow-Headers", "Content-Type,Authorization"),
    ("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS"),
    ("Content-Type",                 "application/json"),
]

def make_response(code, data, extra_headers=None):
    body = json.dumps(data).encode()
    headers = list(CORS)
    headers.append(("Content-Length", str(len(body))))
    if extra_headers:
        headers.extend(extra_headers)
    return code, headers, body

def ok(data, code=200):       return make_response(code, data)
def err(msg, code=400):       return make_response(code, {"error": msg})
def get_tok(headers):
    return (headers.get("Authorization") or "").replace("Bearer ", "").strip()

def read_json_body(body_bytes):
    try:
        return json.loads(body_bytes) if body_bytes else {}
    except Exception:
        return {}

async def http_handler(path, request_headers):
    """
    Called by websockets for every incoming request.
    Return (status, headers, body) to handle as HTTP.
    Return None to let websockets handle it as WebSocket.
    """
    method = request_headers.get("X-Method", "GET").upper()

    # OPTIONS preflight
    if method == "OPTIONS":
        return HTTPStatus.NO_CONTENT, CORS, b""

    # Health check
    if path in ("/", "/health"):
        return ok({"status": "ok", "app": "ThreadTalk v6"})

    token = get_tok(request_headers)

    # ── Auth ──────────────────────────────────────────────────────────────────
    if path == "/api/auth/me" and method == "GET":
        u = resolve_session(token)
        if not u: return err("Not authenticated.", 401)
        return ok({"user": {"id": u["id"], "username": u["username"],
            "display_name": u["display_name"], "color": u["color"],
            "is_admin": bool(u["is_admin"]), "msg_count": u["msg_count"]}})

    # ── Admin ─────────────────────────────────────────────────────────────────
    if path == "/api/admin/stats" and method == "GET":
        u = resolve_session(token)
        if not u or not u.get("is_admin"): return err("Admin only.", 403)
        s = get_stats(); s["online"] = len(ws_clients)
        return ok({"stats": s, "leaderboard": get_leaderboard()})

    if path == "/api/admin/users" and method == "GET":
        u = resolve_session(token)
        if not u or not u.get("is_admin"): return err("Admin only.", 403)
        rows = db_query(
            "SELECT id,username,display_name,color,is_admin,is_banned,"
            "created_at,last_seen,msg_count FROM users ORDER BY created_at DESC")
        return ok({"users": [dict(r) for r in rows]})

    if path == "/api/admin/messages" and method == "GET":
        u = resolve_session(token)
        if not u or not u.get("is_admin"): return err("Admin only.", 403)
        rows = db_query(
            "SELECT id,room,user_name,text,sent_at FROM messages "
            "ORDER BY id DESC LIMIT 100")
        return ok({"messages": [dict(r) for r in rows]})

    if path == "/api/admin/rooms" and method == "GET":
        u = resolve_session(token)
        if not u or not u.get("is_admin"): return err("Admin only.", 403)
        rows = db_query(
            "SELECT r.*,(SELECT COUNT(*) FROM messages m WHERE m.room=r.name) "
            "as msg_count FROM rooms r ORDER BY r.name")
        return ok({"rooms": [dict(r) for r in rows]})

    # These POST/PUT/DELETE routes need body — handled below
    # Return None here so websockets calls process_request which has the body
    return None

# ─── Full HTTP router (has access to body) ────────────────────────────────────
async def process_request(connection, request):
    """
    websockets >= 12: called with (connection, request: websockets.http11.Request)
    Return a Response to short-circuit WebSocket upgrade.
    Return None to proceed with WebSocket.
    """
    from websockets.http11 import Response

    path    = urlparse(request.path).path
    method  = getattr(request, "method", request.headers.get("X-Method", "GET")).upper()
    token   = (request.headers.get("Authorization") or "").replace("Bearer ", "").strip()

    def resp(code, data):
        body = json.dumps(data).encode()
        hdrs = websockets.datastructures.Headers(
            [("Content-Type", "application/json"),
             ("Content-Length", str(len(body))),
             ("Access-Control-Allow-Origin", "*"),
             ("Access-Control-Allow-Headers", "Content-Type,Authorization"),
             ("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")])
        return Response(code, "OK" if code < 400 else "Error", hdrs, body)

    def read_body():
        try:
            body = request.body if hasattr(request, "body") else b""
            return json.loads(body) if body else {}
        except Exception:
            return {}

    # OPTIONS
    if method == "OPTIONS":
        hdrs = websockets.datastructures.Headers([
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Headers", "Content-Type,Authorization"),
            ("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS"),
            ("Content-Length", "0")])
        return Response(204, "No Content", hdrs, b"")

    # Health
    if path in ("/", "/health"):
        return resp(200, {"status": "ok", "app": "ThreadTalk v6"})

    # ── GET routes ────────────────────────────────────────────────────────────
    if method == "GET":
        if path == "/api/auth/me":
            u = resolve_session(token)
            if not u: return resp(401, {"error": "Not authenticated."})
            return resp(200, {"user": {
                "id": u["id"], "username": u["username"],
                "display_name": u["display_name"], "color": u["color"],
                "is_admin": bool(u["is_admin"]), "msg_count": u["msg_count"]}})

        if path == "/api/admin/stats":
            u = resolve_session(token)
            if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})
            s = get_stats(); s["online"] = len(ws_clients)
            return resp(200, {"stats": s, "leaderboard": get_leaderboard()})

        if path == "/api/admin/users":
            u = resolve_session(token)
            if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})
            rows = db_query("SELECT id,username,display_name,color,is_admin,is_banned,created_at,last_seen,msg_count FROM users ORDER BY created_at DESC")
            return resp(200, {"users": [dict(r) for r in rows]})

        if path == "/api/admin/messages":
            u = resolve_session(token)
            if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})
            rows = db_query("SELECT id,room,user_name,text,sent_at FROM messages ORDER BY id DESC LIMIT 100")
            return resp(200, {"messages": [dict(r) for r in rows]})

        if path == "/api/admin/rooms":
            u = resolve_session(token)
            if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})
            rows = db_query("SELECT r.*,(SELECT COUNT(*) FROM messages m WHERE m.room=r.name) as msg_count FROM rooms r ORDER BY r.name")
            return resp(200, {"rooms": [dict(r) for r in rows]})

        # WebSocket upgrade path — let websockets handle it
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return None

        return resp(404, {"error": "Not found."})

    # ── POST routes ───────────────────────────────────────────────────────────
    if method == "POST":
        d = read_body()

        if path == "/api/auth/register":
            username = (d.get("username") or "").strip().lower()[:30]
            display  = (d.get("display_name") or username).strip()[:30]
            password = d.get("password", "")
            if not username or not password:
                return resp(400, {"error": "Username and password required."})
            if len(password) < 6:
                return resp(400, {"error": "Password must be at least 6 characters."})
            if db_one("SELECT id FROM users WHERE username=?", (username,)):
                return resp(409, {"error": "Username already taken."})
            color = pick_color()
            db_exec("INSERT INTO users (username,display_name,password_hash,color,created_at) VALUES (?,?,?,?,?)",
                    (username, display, hash_pw(password), color, iso_now()))
            user  = dict(db_one("SELECT * FROM users WHERE username=?", (username,)))
            tok   = new_token()
            db_exec("INSERT INTO sessions (token,user_id,created_at) VALUES (?,?,?)",
                    (tok, user["id"], iso_now()))
            print(f"[auth] + {display}")
            return resp(201, {"token": tok, "user": {
                "id": user["id"], "username": username,
                "display_name": display, "color": color, "is_admin": False}})

        if path == "/api/auth/login":
            username = (d.get("username") or "").strip().lower()
            password = d.get("password", "")
            user = db_one(
                "SELECT * FROM users WHERE username=? AND password_hash=?",
                (username, hash_pw(password)))
            if not user: return resp(401, {"error": "Invalid username or password."})
            user = dict(user)
            if user.get("is_banned"): return resp(403, {"error": "Account banned."})
            tok = new_token()
            db_exec("INSERT INTO sessions (token,user_id,created_at) VALUES (?,?,?)",
                    (tok, user["id"], iso_now()))
            db_exec("UPDATE users SET last_seen=? WHERE id=?", (iso_now(), user["id"]))
            with sessions_lock: sessions[tok] = user
            print(f"[auth] → {user['display_name']}")
            return resp(200, {"token": tok, "user": {
                "id": user["id"], "username": user["username"],
                "display_name": user["display_name"], "color": user["color"],
                "is_admin": bool(user["is_admin"])}})

        if path == "/api/auth/logout":
            db_exec("DELETE FROM sessions WHERE token=?", (token,))
            with sessions_lock: sessions.pop(token, None)
            return resp(200, {"ok": True})

        if path == "/api/admin/rooms":
            u = resolve_session(token)
            if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})
            name  = (d.get("name") or "").strip().lower().replace(" ", "-")[:20]
            topic = (d.get("topic") or "").strip()[:80]
            if not name: return resp(400, {"error": "Room name required."})
            if db_one("SELECT id FROM rooms WHERE name=?", (name,)):
                return resp(409, {"error": "Room already exists."})
            db_exec("INSERT INTO rooms (name,topic,created_by,created_at) VALUES (?,?,?,?)",
                    (name, topic, u["display_name"], iso_now()))
            return resp(201, {"ok": True, "room": name})

        return resp(404, {"error": "Not found."})

    # ── PUT routes ────────────────────────────────────────────────────────────
    if method == "PUT":
        u = resolve_session(token)
        if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})
        d = read_body()

        if path.startswith("/api/admin/users/") and path.endswith("/ban"):
            uid = path.split("/")[4]
            if uid == str(u["id"]): return resp(400, {"error": "Cannot ban yourself."})
            db_exec("UPDATE users SET is_banned=? WHERE id=?",
                    (1 if d.get("banned") else 0, uid))
            return resp(200, {"ok": True})

        if path.startswith("/api/admin/users/") and path.endswith("/admin"):
            uid = path.split("/")[4]
            db_exec("UPDATE users SET is_admin=? WHERE id=?",
                    (1 if d.get("is_admin") else 0, uid))
            return resp(200, {"ok": True})

        return resp(404, {"error": "Not found."})

    # ── DELETE routes ─────────────────────────────────────────────────────────
    if method == "DELETE":
        u = resolve_session(token)
        if not u or not u.get("is_admin"): return resp(403, {"error": "Admin only."})

        if path.startswith("/api/admin/messages/"):
            db_exec("DELETE FROM messages WHERE id=?", (path.split("/")[4],))
            return resp(200, {"ok": True})

        if path.startswith("/api/admin/rooms/"):
            rname = path.split("/")[4]
            if rname == "general":
                return resp(400, {"error": "Cannot delete #general."})
            db_exec("DELETE FROM rooms WHERE name=?", (rname,))
            return resp(200, {"ok": True})

        return resp(404, {"error": "Not found."})

    return resp(405, {"error": "Method not allowed."})

# ─── Entry point ──────────────────────────────────────────────────────────────
async def main():
    init_db()
    print(f"""
╔══════════════════════════════════════════════════╗
║  ThreadTalk v6  —  Single Port, Render Ready     ║
║  Port    : {PORT}                                ║
║  WS path : wss://your-app.onrender.com/ws        ║
║  API     : https://your-app.onrender.com/api/*   ║
║  Admin   : admin / admin123                      ║
╚══════════════════════════════════════════════════╝
""")
    async with serve(
        ws_handler,
        "0.0.0.0",
        PORT,
        process_request=process_request,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"[server] Listening on :{PORT} …")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[server] Bye.")