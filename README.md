# ThreadTalk — Threads · Sockets · SQLite · Auth
## Final Project

---

## How to Run

```bash
# 1 — Start the all-in-one server (chat TCP + REST API)
python server.py

# 2 — Start the WebSocket bridge (so browser can reach TCP server)
pip install websockets   # once
python bridge.py

# 3 — Open login.html in your browser
```

### Default Admin
| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `admin123` |

---

## File Map

| File | Role |
|------|------|
| `server.py` | All-in-one server: TCP chat (threads+sockets) + HTTP REST API |
| `bridge.py` | WebSocket ↔ TCP bridge |
| `login.html` | Sign in page |
| `signup.html` | Registration page with password strength + color picker |
| `chat.html` | Main chat UI (requires login token) |
| `admin.html` | Admin dashboard: users, messages, rooms, leaderboard |

---

## Architecture

```
browser
  │
  ├── login.html / signup.html
  │       └── POST /auth/login or /auth/register  → HTTP :9000
  │               └── returns session token
  │
  ├── chat.html
  │       ├── GET /auth/me  → verify token
  │       └── WebSocket :8765  →  bridge.py  →  TCP :9001
  │                                               (one thread per client)
  │
  └── admin.html
          ├── GET  /admin/users, /admin/messages, /admin/rooms, /admin/stats
          ├── PUT  /admin/users/:id/ban, /admin/users/:id/admin
          ├── POST /admin/rooms
          └── DELETE /admin/messages/:id, /admin/rooms/:name
```

---

## Threads & Sockets Demonstrated

| Concept | Location |
|---------|----------|
| `socket.socket(AF_INET, SOCK_STREAM)` | `server.py → run_tcp()` |
| `server.listen()` / `server.accept()` | `server.py → run_tcp()` |
| `threading.Thread(target=handle_client)` | One thread spawned per TCP client |
| `threading.Lock()` on `clients` dict | Thread-safe online user tracking |
| `threading.Lock()` on SQLite connection | Thread-safe DB writes |
| HTTP server on separate thread | `threading.Thread(target=run_http)` |
| Per-room broadcast | Iterates locked clients dict |

---

## Database Schema

```sql
users    — id, username, display_name, password_hash, color, is_admin, is_banned, created_at, last_seen, msg_count
rooms    — id, name, topic, created_by, created_at
messages — id, room, user_id, user_name, text, sent_at
sessions — token, user_id, created_at
```
