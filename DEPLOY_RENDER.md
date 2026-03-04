# Deploy Eko Football to Render

Use this guide after your code is on GitHub (timkene/EKO-FITNESS).

---

## 0. Push local FOOTBALL data to MotherDuck (one-time)

So Render uses the same members/matchdays as your local app:

1. In the **DLT** project (not EKO-FITNESS-app), from project root:
   - Inspect local schema: `python scripts/inspect_football_schema.py`
   - Push to MotherDuck: `MOTHERDUCK_TOKEN=your_token python scripts/push_football_to_motherduck.py`
2. Create a **MotherDuck token** at [motherduck.com](https://motherduck.com) → Account → Personal Access Token (read/write).
3. Use that token in the push command above and in Render (step 1.5).

---

## 1. Create a Web Service on Render

1. Go to **https://dashboard.render.com** and sign in (or sign up with GitHub).
2. Click **New +** → **Web Service**.
3. Connect your GitHub account if needed, then select the repo **timkene/EKO-FITNESS**.
4. Use these settings:

   | Setting | Value |
   |--------|--------|
   | **Name** | `eko-football-api` (or any name) |
   | **Region** | Choose closest to you |
   | **Branch** | `main` |
   | **Root Directory** | Leave blank |
   | **Runtime** | `Python 3` |
   | **Build Command** | `pip install -r requirements.txt` |
   | **Start Command** | `uvicorn main:app --host 0.0.0.0 --port $PORT` |

5. **Environment (production with MotherDuck – recommended):**
   - `USE_LOCAL_DB` = `false`
   - `MOTHERDUCK_TOKEN` = your MotherDuck API token (create at [motherduck.com](https://motherduck.com) → Account → Personal Access Token).
   - Do **not** commit the token. Set it only in Render Dashboard → your service → Environment.
   - If you keep `USE_LOCAL_DB` = `true`, Render will use an empty local DB (no members/matchdays).

6. Click **Create Web Service**. Render will build and deploy. Wait until the service shows **Live** and note the URL (e.g. `https://eko-football-api.onrender.com`).

---

## 2. (Optional) Deploy the React frontend

The repo has both backend (`main.py`) and frontend (`eko-react/`). You can:

**Option A – Backend only for now**  
Use the API URL from step 1. Point your local React app at it by setting `VITE_FOOTBALL_API_URL=https://your-service.onrender.com/api/v1/football` when you run the frontend locally.

**Option B – Frontend on Render as well**  
1. **New +** → **Static Site**. Connect repo **timkene/EKO-FITNESS**.  
2. **Name:** `eko-fitness-app` (so CORS is already allowed). **Branch:** `main`. **Root Directory:** `eko-react`.  
3. **Build Command:** `npm install && npm run build`. **Publish Directory:** `dist`.  
4. **Environment:** `VITE_FOOTBALL_API_URL` = `https://eko-football-api.onrender.com/api/v1/football` (use your backend URL if different).  
5. Create. App will be at **https://eko-fitness-app.onrender.com**.  
6. **Fix blank page on refresh:** In Render Dashboard → your **Static Site** → **Redirects/Rewrites** → **Add Rule**: Source `/*`, Destination `/index.html`, Action **Rewrite**. Save. This makes every path serve `index.html` so React Router can handle routes.

---

## 3. CORS

The backend allows `localhost:5173`–`5176` and `https://eko-fitness-app.onrender.com`. If you use a different frontend URL on Render, add it to allow_origins in main.py and redeploy. Original note: you’ll need to add that origin in the backend (in `main.py` or `main_eko.py`, in `allow_origins`). If your frontend is on Render, add that URL (e.g. `https://eko-football.onrender.com`) to the list.

---

## Quick reference

- **Backend start command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Build:** `pip install -r requirements.txt`
- **Repo:** https://github.com/timkene/EKO-FITNESS
