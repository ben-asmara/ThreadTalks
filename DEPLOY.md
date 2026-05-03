# Deploying ThreadTalk to Render (Free)

## Step 1 — Push everything to GitHub

Make sure ALL these files are committed and pushed to your repo:

```
server.py         ← new unified cloud server
render.yaml       ← Render config
requirements.txt  ← (empty, stdlib only)
login.html
signup.html
chat.html
admin.html
```

```bash
git add .
git commit -m "Cloud edition: unified server for Render deploy"
git push
```

---

## Step 2 — Deploy the Python server on Render

1. Go to **https://render.com** and sign up (free)
2. Click **"New +"** → **"Web Service"**
3. Connect your GitHub account → select **ThreadTalks** repo
4. Fill in the settings:

| Field | Value |
|-------|-------|
| Name | `threadtalk` (or anything) |
| Runtime | `Python 3` |
| Build Command | `pip install --upgrade pip` |
| Start Command | `python server.py` |
| Instance Type | **Free** |

5. Click **"Create Web Service"**
6. Wait ~2 minutes for it to build and deploy
7. Render gives you a URL like: `https://threadtalk.onrender.com`

---

## Step 3 — Enable GitHub Pages for the HTML files

1. In your GitHub repo → **Settings** → **Pages**
2. Source: **Deploy from a branch**
3. Branch: **main** / root
4. Save → GitHub gives you: `https://ben-asmara.github.io/ThreadTalks/`

---

## Step 4 — Update the HTML files with your Render URL

In your HTML files, replace `BACKEND_URL` and `BACKEND_WS_URL`:

### In `login.html` and `signup.html`:
```js
// Change:
const API = "BACKEND_URL";
// To:
const API = "https://threadtalk.onrender.com";
```

### In `admin.html`:
```js
// Change:
const API="BACKEND_URL";
// To:
const API="https://threadtalk.onrender.com";
```

### In `chat.html`:
```js
// Change:
const API = "BACKEND_URL";
ws = new WebSocket("BACKEND_WS_URL");
// To:
const API = "https://threadtalk.onrender.com";
ws = new WebSocket("wss://threadtalk.onrender.com/ws");
```

> ⚠️ Note: Use **wss://** (secure WebSocket) not ws:// for HTTPS sites.

Then push again:
```bash
git add .
git commit -m "Set live Render URL"
git push
```

---

## Step 5 — Done! Test it

| Page | URL |
|------|-----|
| Login | `https://ben-asmara.github.io/ThreadTalks/login.html` |
| Sign Up | `https://ben-asmara.github.io/ThreadTalks/signup.html` |
| Chat | `https://ben-asmara.github.io/ThreadTalks/chat.html` |
| Admin | `https://ben-asmara.github.io/ThreadTalks/admin.html` |

Default admin: **admin / admin123**

---

## ⚠️ Free tier notes

- Render free services **spin down after 15 min of inactivity** — the first request after idle takes ~30 seconds to wake up. This is normal on the free plan.
- The SQLite database resets if Render redeploys. For a permanent database, upgrade to a paid plan or use Render's PostgreSQL (free tier available).
