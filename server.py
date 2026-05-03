"""
=============================================================
  ThreadTalk v3 — Threads + Sockets + SQLite + Auth + Admin
=============================================================
  Run:  python server.py
  Port: 9001

  First user to register automatically becomes ADMIN.
  Admin commands (sent as chat messages):
    /kick <name>      — disconnect a user
    /ban <name>       — ban a user permanently
    /unban <name>     — unban a user
    /mute <name>      — mute a user (cannot send messages)
    /unmute <name>    — unmute a user
    /addroom <name>   — create a new room
    /delroom <name>   — delete a room
    /broadcast <msg>  — send to ALL rooms
    /stats            — print server stats
=============================================================
"""

import socket, threading, sqlite3, json, sys, hashlib, secrets, time
from datetime import datetime

HOST    = "0.0.0.0"
PORT    = 9001
DB_FILE = "threadtalk.db"

clients      = {}   # conn -> {name, addr, room, color, role, session}
clients_lock = threading.Lock()

COLORS = ["#6c63ff","#00e5c3","#f59e0b","#ec4899","#3b82f6",
          "#10b981","#f97316","#a855f7","#ef4444","#06b6d4"]

# ─── DB Setup ─────────────────────────────────────────────────────────────────
_db_lock = threading.Lock()
_db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)
_db_conn.row_factory = sqlite3.Row
_db_conn.execute("PRAGMA journal_mode=WAL")

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
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT UNIQUE NOT NULL,
        password     TEXT NOT NULL,
        color        TEXT NOT NULL DEFAULT '#6c63ff',
        role         TEXT NOT NULL DEFAULT 'user',
        status       TEXT NOT NULL DEFAULT 'active',
        msg_count    INTEGER DEFAULT 0,
        created_at   TEXT NOT NULL,
        last_seen    TEXT
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS sessions (
        token      TEXT PRIMARY KEY,
        user_name  TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS rooms (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        name       TEXT UNIQUE NOT NULL,
        created_by TEXT DEFAULT 'system',
        created_at TEXT NOT NULL
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS messages (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        room      TEXT NOT NULL,
        user_name TEXT NOT NULL,
        text      TEXT NOT NULL,
        sent_at   TEXT NOT NULL,
        deleted   INTEGER DEFAULT 0
    )""")
    db_exec("""CREATE TABLE IF NOT EXISTS audit_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        actor      TEXT NOT NULL,
        action     TEXT NOT NULL,
        target     TEXT,
        detail     TEXT,
        created_at TEXT NOT NULL
    )""")
    for room in ["general","random","tech","announcements"]:
        db_exec("INSERT OR IGNORE INTO rooms (name,created_by,created_at) VALUES (?,?,?)",
                (room,"system",iso_now()))
    print("[db] Database ready ✓  →  " + DB_FILE)

# ─── Time helpers ─────────────────────────────────────────────────────────────
def iso_now(): return datetime.now().isoformat(timespec="seconds")
def ts():      return datetime.now().strftime("%H:%M")

# ─── Auth helpers ─────────────────────────────────────────────────────────────
def hash_pw(password):
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(name, password):
    existing = db_one("SELECT id FROM users WHERE name=?", (name,))
    if existing:
        return None, "Username already taken."
    count = db_one("SELECT COUNT(*) as c FROM users")[0]
    role  = "admin" if count == 0 else "user"
    color = COLORS[count % len(COLORS)]
    db_exec("""INSERT INTO users (name,password,color,role,status,created_at)
               VALUES (?,?,?,?,?,?)""",
            (name, hash_pw(password), color, role, "active", iso_now()))
    audit("system","register",name,f"role={role}")
    return db_one("SELECT * FROM users WHERE name=?", (name,)), None

def login_user(name, password):
    row = db_one("SELECT * FROM users WHERE name=?", (name,))
    if not row:
        return None, "User not found."
    if row["password"] != hash_pw(password):
        return None, "Wrong password."
    if row["status"] == "banned":
        return None, "Your account has been banned."
    token = secrets.token_hex(32)
    expires = datetime.now().replace(hour=23,minute=59,second=59).isoformat()
    db_exec("INSERT INTO sessions (token,user_name,created_at,expires_at) VALUES (?,?,?,?)",
            (token, name, iso_now(), expires))
    db_exec("UPDATE users SET last_seen=? WHERE name=?", (iso_now(), name))
    return {"user": dict(row), "token": token}, None

def validate_session(token):
    row = db_one("SELECT * FROM sessions WHERE token=?", (token,))
    if not row: return None
    user = db_one("SELECT * FROM users WHERE name=?", (row["user_name"],))
    if not user or user["status"] == "banned": return None
    return dict(user)

def logout_session(token):
    db_exec("DELETE FROM sessions WHERE token=?", (token,))

# ─── Data helpers ─────────────────────────────────────────────────────────────
def save_message(room, user_name, text):
    db_exec("INSERT INTO messages (room,user_name,text,sent_at) VALUES (?,?,?,?)",
            (room, user_name, text, iso_now()))
    db_exec("UPDATE users SET msg_count=msg_count+1, last_seen=? WHERE name=?",
            (iso_now(), user_name))

def get_history(room, limit=40):
    rows = db_query("""SELECT m.user_name, m.text, m.sent_at, u.color, u.role
                       FROM messages m LEFT JOIN users u ON m.user_name=u.name
                       WHERE m.room=? AND m.deleted=0
                       ORDER BY m.id DESC LIMIT ?""", (room, limit))
    return list(reversed([dict(r) for r in rows]))

def get_rooms():
    return [r["name"] for r in db_query("SELECT name FROM rooms ORDER BY name")]

def get_all_users():
    rows = db_query("""SELECT name,color,role,status,msg_count,created_at,last_seen
                       FROM users ORDER BY created_at DESC""")
    return [dict(r) for r in rows]

def get_leaderboard():
    rows = db_query("""SELECT name,color,role,msg_count FROM users
                       WHERE status='active' ORDER BY msg_count DESC LIMIT 10""")
    return [dict(r) for r in rows]

def get_stats():
    return {
        "messages": db_one("SELECT COUNT(*) as c FROM messages WHERE deleted=0")[0],
        "users":    db_one("SELECT COUNT(*) as c FROM users")[0],
        "rooms":    db_one("SELECT COUNT(*) as c FROM rooms")[0],
        "banned":   db_one("SELECT COUNT(*) as c FROM users WHERE status='banned'")[0],
        "muted":    db_one("SELECT COUNT(*) as c FROM users WHERE status='muted'")[0],
    }

def get_audit_log(limit=50):
    rows = db_query("""SELECT actor,action,target,detail,created_at
                       FROM audit_log ORDER BY id DESC LIMIT ?""", (limit,))
    return [dict(r) for r in rows]

def audit(actor, action, target=None, detail=None):
    db_exec("INSERT INTO audit_log (actor,action,target,detail,created_at) VALUES (?,?,?,?,?)",
            (actor, action, target, detail, iso_now()))

def is_muted(name):
    row = db_one("SELECT status FROM users WHERE name=?", (name,))
    return row and row["status"] == "muted"

# ─── Network ──────────────────────────────────────────────────────────────────
def send_to(conn, payload):
    try: conn.sendall((json.dumps(payload)+"\n").encode())
    except OSError: remove_client(conn)

def broadcast(payload, room=None, exclude_conn=None):
    data = (json.dumps(payload)+"\n").encode()
    dead = []
    with clients_lock:
        for conn, info in clients.items():
            if conn is exclude_conn: continue
            if room and info.get("room") != room: continue
            try: conn.sendall(data)
            except OSError: dead.append(conn)
    for c in dead: remove_client(c)

def broadcast_all(payload):
    """Send to every connected client regardless of room."""
    data = (json.dumps(payload)+"\n").encode()
    dead = []
    with clients_lock:
        for conn in list(clients):
            try: conn.sendall(data)
            except OSError: dead.append(conn)
    for c in dead: remove_client(c)

def user_list(room=None):
    with clients_lock:
        return [{"name":v["name"],"color":v["color"],"role":v["role"]}
                for v in clients.values()
                if room is None or v.get("room")==room]

def find_conn_by_name(name):
    with clients_lock:
        for conn, info in clients.items():
            if info["name"] == name: return conn
    return None

def remove_client(conn):
    with clients_lock:
        info = clients.pop(conn, None)
    if not info: return
    try: conn.close()
    except OSError: pass
    room = info.get("room","general")
    print(f"[{ts()}] ✗ {info['name']} left #{room}")
    broadcast({"type":"system","text":f"{info['name']} left.",
               "time":ts(),"users":user_list(room),"room":room,
               "stats":get_stats()}, room=room)

# ─── Admin commands ───────────────────────────────────────────────────────────
def handle_command(conn, info, text):
    """Returns True if text was an admin command."""
    if not text.startswith("/"): return False
    parts = text[1:].split(" ", 1)
    cmd   = parts[0].lower()
    arg   = parts[1].strip() if len(parts)>1 else ""
    name  = info["name"]
    role  = info["role"]

    def reply(msg):
        send_to(conn, {"type":"system","text":msg,"time":ts(),"room":info.get("room","general")})

    if role != "admin":
        reply("⛔ Admin only command.")
        return True

    if cmd == "kick":
        target_conn = find_conn_by_name(arg)
        if target_conn:
            send_to(target_conn, {"type":"kicked","text":"You were kicked by an admin."})
            remove_client(target_conn)
            audit(name,"kick",arg)
            reply(f"✓ Kicked {arg}.")
        else: reply(f"User {arg} not online.")

    elif cmd == "ban":
        db_exec("UPDATE users SET status='banned' WHERE name=?", (arg,))
        target_conn = find_conn_by_name(arg)
        if target_conn:
            send_to(target_conn,{"type":"banned","text":"You have been banned."})
            remove_client(target_conn)
        audit(name,"ban",arg)
        reply(f"✓ Banned {arg}.")

    elif cmd == "unban":
        db_exec("UPDATE users SET status='active' WHERE name=?", (arg,))
        audit(name,"unban",arg)
        reply(f"✓ Unbanned {arg}.")

    elif cmd == "mute":
        db_exec("UPDATE users SET status='muted' WHERE name=?", (arg,))
        audit(name,"mute",arg)
        reply(f"✓ Muted {arg}.")
        target_conn = find_conn_by_name(arg)
        if target_conn:
            send_to(target_conn,{"type":"system","text":"You have been muted by an admin.","time":ts(),"room":info.get("room")})

    elif cmd == "unmute":
        db_exec("UPDATE users SET status='active' WHERE name=?", (arg,))
        audit(name,"unmute",arg)
        reply(f"✓ Unmuted {arg}.")

    elif cmd == "addroom":
        roomname = arg.lower().replace(" ","_")[:20]
        try:
            db_exec("INSERT INTO rooms (name,created_by,created_at) VALUES (?,?,?)",
                    (roomname, name, iso_now()))
            audit(name,"addroom",roomname)
            broadcast_all({"type":"rooms_updated","rooms":get_rooms()})
            reply(f"✓ Room #{roomname} created.")
        except: reply("Room already exists.")

    elif cmd == "delroom":
        if arg in ("general","announcements"):
            reply("Cannot delete core rooms.")
        else:
            db_exec("DELETE FROM rooms WHERE name=?", (arg,))
            audit(name,"delroom",arg)
            broadcast_all({"type":"rooms_updated","rooms":get_rooms()})
            reply(f"✓ Room #{arg} deleted.")

    elif cmd == "broadcast":
        broadcast_all({"type":"broadcast","text":f"📢 [Admin] {arg}","time":ts(),"name":name})
        audit(name,"broadcast",None,arg)

    elif cmd == "stats":
        s = get_stats()
        reply(f"📊 msgs:{s['messages']} users:{s['users']} online:{len(clients)} banned:{s['banned']} muted:{s['muted']}")

    elif cmd == "promote":
        db_exec("UPDATE users SET role='admin' WHERE name=?", (arg,))
        audit(name,"promote",arg)
        reply(f"✓ {arg} promoted to admin.")

    elif cmd == "demote":
        db_exec("UPDATE users SET role='user' WHERE name=?", (arg,))
        audit(name,"demote",arg)
        reply(f"✓ {arg} demoted to user.")

    else:
        reply(f"Unknown command /{cmd}")

    return True

# ─── Per-client thread ────────────────────────────────────────────────────────
def handle_client(conn, addr):
    tname = threading.current_thread().name
    print(f"[{ts()}] ✔ {addr} [{tname}]")
    buf = ""

    def recv_line():
        nonlocal buf
        while True:
            chunk = conn.recv(2048).decode("utf-8", errors="replace")
            if not chunk: raise ConnectionResetError
            buf += chunk
            if "\n" in buf:
                line, buf = buf.split("\n", 1)
                return json.loads(line.strip())

    try:
        # ── Auth handshake ──
        pkt = recv_line()
        action = pkt.get("type")

        if action == "register":
            user_row, err = register_user(pkt.get("name",""), pkt.get("password",""))
            if err:
                send_to(conn, {"type":"auth_error","text":err})
                conn.close(); return
            # auto-login after register
            result, _ = login_user(pkt["name"], pkt["password"])
            user = result["user"]; token = result["token"]
            send_to(conn, {"type":"auth_ok","name":user["name"],"color":user["color"],
                           "role":user["role"],"token":token,
                           "text":f"Account created! Welcome, {user['name']}."})

        elif action == "login":
            result, err = login_user(pkt.get("name",""), pkt.get("password",""))
            if err:
                send_to(conn, {"type":"auth_error","text":err})
                conn.close(); return
            user = result["user"]; token = result["token"]
            send_to(conn, {"type":"auth_ok","name":user["name"],"color":user["color"],
                           "role":user["role"],"token":token,
                           "text":f"Welcome back, {user['name']}!"})

        elif action == "resume":
            user = validate_session(pkt.get("token",""))
            if not user:
                send_to(conn, {"type":"auth_error","text":"Session expired. Please log in again."})
                conn.close(); return
            token = pkt["token"]
            send_to(conn, {"type":"auth_ok","name":user["name"],"color":user["color"],
                           "role":user["role"],"token":token,
                           "text":f"Session restored. Welcome back, {user['name']}!"})
        else:
            send_to(conn, {"type":"auth_error","text":"Please log in first."})
            conn.close(); return

        # ── Join a room ──
        pkt2 = recv_line()
        if pkt2.get("type") != "join":
            conn.close(); return
        room = pkt2.get("room","general")
        if room not in get_rooms(): room = "general"

        name  = user["name"]
        color = user["color"]
        role  = user["role"]

        with clients_lock:
            clients[conn] = {"name":name,"addr":addr,"room":room,
                             "color":color,"role":role,"token":token}

        print(f"[{ts()}] ★  {name} ({role}) → #{room} [{tname}]")

        send_to(conn, {
            "type":"welcome","name":name,"color":color,"role":role,"room":room,
            "rooms":get_rooms(),"history":get_history(room),
            "users":user_list(room),"stats":get_stats(),
            "leaderboard":get_leaderboard(),"time":ts(),
            "msg_count":user["msg_count"],
            "text":f"Joined #{room}. You have sent {user['msg_count']} messages.",
        })

        broadcast({"type":"system","text":f"{name} joined #{room}.","time":ts(),
                   "users":user_list(room),"room":room,"stats":get_stats(),
                   "leaderboard":get_leaderboard()}, room=room, exclude_conn=conn)

        # ── Message loop ──
        while True:
            pkt = recv_line()
            mtype = pkt.get("type")

            if mtype == "message":
                text = pkt.get("text","").strip()
                if not text: continue
                cur_room = clients.get(conn,{}).get("room","general")

                if handle_command(conn, clients.get(conn,{}), text):
                    continue

                if is_muted(name):
                    send_to(conn,{"type":"system","text":"You are muted.","time":ts(),"room":cur_room})
                    continue

                save_message(cur_room, name, text)
                payload = {"type":"message","name":name,"color":color,"role":role,
                           "text":text,"time":ts(),"room":cur_room,
                           "leaderboard":get_leaderboard(),"stats":get_stats()}
                send_to(conn, payload)
                broadcast(payload, room=cur_room, exclude_conn=conn)

            elif mtype == "switch_room":
                new_room = pkt.get("room","general")
                if new_room not in get_rooms(): continue
                old_room = clients[conn].get("room","general")
                with clients_lock: clients[conn]["room"] = new_room
                broadcast({"type":"system","text":f"{name} left.","time":ts(),
                           "users":user_list(old_room),"room":old_room}, room=old_room)
                send_to(conn,{"type":"room_switched","room":new_room,
                              "history":get_history(new_room),"users":user_list(new_room),"time":ts()})
                broadcast({"type":"system","text":f"{name} joined.","time":ts(),
                           "users":user_list(new_room),"room":new_room}, room=new_room, exclude_conn=conn)

            elif mtype == "typing":
                cur_room = clients.get(conn,{}).get("room","general")
                broadcast({"type":"typing","name":name,"active":pkt.get("active",False),"room":cur_room},
                          room=cur_room, exclude_conn=conn)

            elif mtype == "admin_data":
                if role == "admin":
                    send_to(conn,{"type":"admin_data","users":get_all_users(),
                                  "audit":get_audit_log(),"stats":get_stats(),
                                  "rooms":get_rooms(),"online":user_list()})

            elif mtype == "admin_action":
                if role != "admin": continue
                act = pkt.get("action"); target = pkt.get("target","")
                if act == "ban":
                    db_exec("UPDATE users SET status='banned' WHERE name=?", (target,))
                    tc = find_conn_by_name(target)
                    if tc:
                        send_to(tc,{"type":"banned","text":"You have been banned."})
                        remove_client(tc)
                    audit(name,"ban",target)
                elif act == "unban":
                    db_exec("UPDATE users SET status='active' WHERE name=?", (target,))
                    audit(name,"unban",target)
                elif act == "mute":
                    db_exec("UPDATE users SET status='muted' WHERE name=?", (target,))
                    audit(name,"mute",target)
                elif act == "unmute":
                    db_exec("UPDATE users SET status='active' WHERE name=?", (target,))
                    audit(name,"unmute",target)
                elif act == "kick":
                    tc = find_conn_by_name(target)
                    if tc:
                        send_to(tc,{"type":"kicked","text":"Kicked by admin."})
                        remove_client(tc)
                    audit(name,"kick",target)
                elif act == "promote":
                    db_exec("UPDATE users SET role='admin' WHERE name=?", (target,))
                    audit(name,"promote",target)
                elif act == "demote":
                    db_exec("UPDATE users SET role='user' WHERE name=?", (target,))
                    audit(name,"demote",target)
                elif act == "delete_room":
                    if target not in ("general","announcements"):
                        db_exec("DELETE FROM rooms WHERE name=?", (target,))
                        broadcast_all({"type":"rooms_updated","rooms":get_rooms()})
                        audit(name,"delete_room",target)
                elif act == "add_room":
                    rn = target.lower().replace(" ","_")[:20]
                    try:
                        db_exec("INSERT INTO rooms (name,created_by,created_at) VALUES (?,?,?)",
                                (rn,name,iso_now()))
                        broadcast_all({"type":"rooms_updated","rooms":get_rooms()})
                        audit(name,"add_room",rn)
                    except: pass
                # refresh admin panel
                send_to(conn,{"type":"admin_data","users":get_all_users(),
                              "audit":get_audit_log(),"stats":get_stats(),
                              "rooms":get_rooms(),"online":user_list()})

            elif mtype == "logout":
                logout_session(token)
                send_to(conn,{"type":"logged_out"})
                break

    except (ConnectionResetError, OSError, json.JSONDecodeError):
        pass
    remove_client(conn)

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    init_db()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(50)
    print(f"""
╔════════════════════════════════════════════════════╗
║  ThreadTalk v3  —  Threads + Sockets + Auth + DB   ║
║  TCP  : {HOST}:{PORT}                              ║
║  DB   : {DB_FILE}                       ║
║  First registered user becomes ADMIN               ║
╚════════════════════════════════════════════════════╝
""")
    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=handle_client, args=(conn,addr),
                                 daemon=True, name=f"T-{addr[1]}")
            t.start()
    except KeyboardInterrupt:
        print("\n[server] Shutting down.")
        srv.close(); sys.exit(0)

if __name__ == "__main__":
    main()
