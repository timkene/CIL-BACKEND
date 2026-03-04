"""
batch_band.py  —  Quick one-time script
Runs all providers in a CSV through the banding API and adds tariff_band + reality_band columns.
Usage: python batch_band.py providers.csv
"""

import sys
import requests
import pandas as pd

API = "http://localhost:8000"

# ── Load file ─────────────────────────────────────────────────────────────────
input_file = sys.argv[1] if len(sys.argv) > 1 else "providers.csv"
df = pd.read_csv(input_file)

# Insert the two new columns before "Category"
cat_idx = df.columns.get_loc("Category")
df.insert(cat_idx, "Reality Band", None)
df.insert(cat_idx, "Tariff Band", None)

# ── Fetch provider list from API (for name matching) ──────────────────────────
print("Loading provider list from API...")
known = {p["providername"].lower().strip(): p["providername"]
         for p in requests.get(f"{API}/providers", timeout=15).json()}

# ── Loop ──────────────────────────────────────────────────────────────────────
total = len(df)
found = skipped = 0

for i, row in df.iterrows():
    raw_name = str(row["Provider Name"]).strip()
    matched = known.get(raw_name.lower())

    if not matched:
        print(f"[{i+1}/{total}] SKIP  — not found: {raw_name}")
        skipped += 1
        continue

    try:
        r = requests.post(
            f"{API}/analyze",
            data={"provider_name": matched, "lookback_months": 6},
            timeout=120,
        )
        if r.status_code == 200:
            data = r.json()
            df.at[i, "Tariff Band"]  = data["tariff_banding"]["weighted"]["band"]
            df.at[i, "Reality Band"] = (
                data["reality_banding"]["weighted"]["band"]
                if data.get("reality_banding") else "N/A"
            )
            print(f"[{i+1}/{total}] OK    — {matched} | T:{df.at[i,'Tariff Band']} R:{df.at[i,'Reality Band']}")
            found += 1
        else:
            print(f"[{i+1}/{total}] ERROR — {matched}: {r.status_code}")
            skipped += 1
    except Exception as e:
        print(f"[{i+1}/{total}] ERROR — {raw_name}: {e}")
        skipped += 1

# ── Save ──────────────────────────────────────────────────────────────────────
out = input_file.replace(".csv", "_banded.csv")
df.to_csv(out, index=False)
print(f"\nDone. {found} updated, {skipped} skipped → {out}")