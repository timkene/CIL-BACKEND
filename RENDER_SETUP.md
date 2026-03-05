# Render deployment – use MotherDuck (cloud)

The API **already** uses MotherDuck when not using local DuckDB. No code change is required.

## Environment variables on Render

In your Render **Web Service** (and any **Cron Job** that runs the night batch), set:

| Variable          | Value                    | Required |
|-------------------|--------------------------|----------|
| `USE_LOCAL_DB`    | `false`                  | Yes      |
| `MOTHERDUCK_TOKEN`| Your MotherDuck PAT/token| Yes      |

- With `USE_LOCAL_DB=false`, `core/database.py` connects to **MotherDuck** (database name: `ai_driven_data`).
- With `USE_LOCAL_DB=true` (default), it uses the local `ai_driven_data.duckdb` file (for local dev only).

Render cannot see your local DuckDB file; it **must** use MotherDuck, so set the two variables above.

## Tables in MotherDuck

- **No new summary tables** were created in local DuckDB; the app still computes on demand from the same source tables.
- Ensure all **source tables** your app needs are present in MotherDuck (e.g. by running `auto_update_database.py` with MotherDuck sync, or your existing sync process).  
  If you later add new tables locally, replicate them to MotherDuck so Render sees the same schema and data.

## Night batch

- Run your night batch (e.g. `auto_update_database.py` or a dedicated summary job) as a **Render Cron Job** or **Background Worker**.
- Give it the **same** env vars: `USE_LOCAL_DB=false` and `MOTHERDUCK_TOKEN`.  
  Then the batch runs against MotherDuck and stays in sync with the API.

## Avoid OOM (512MB) on free/starter tier

The default `requirements.txt` includes **streamlit** and **plotly** (used by the Tariff route). Loading them can exceed 512MB and cause "Out of memory" / "No open ports detected".

**Do this on Render:**

1. **Build Command** (in Render Dashboard → Service → Settings → Build & Deploy):
   - Set to: `pip install -r requirements-render.txt`
   - This installs the same deps as `requirements.txt` but **without** streamlit/plotly. The Tariff route is disabled on Render automatically.

2. **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`

3. **Environment**: `USE_LOCAL_DB=false`, `MOTHERDUCK_TOKEN=<your_token>`

If you need more headroom, upgrade the instance to 1GB+ RAM or use the summary-table architecture (precompute → read from summary tables).

## Quick checklist

1. Create the Web Service (and Cron/Worker if needed) on Render from the CIL-BACKEND repo.
2. Set **Build Command** to `pip install -r requirements-render.txt` (see above).
3. Set `USE_LOCAL_DB=false` and `MOTHERDUCK_TOKEN=<your_token>` in the service environment.
4. Deploy. The API will connect to MotherDuck; Tariff route is disabled on Render to save memory.
