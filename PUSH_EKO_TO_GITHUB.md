# Push only the Eko React app to your EKO-FITNESS repo

Your Eko Football app lives inside the DLT folder. These steps put **only** the Eko app into a new folder and push it to **EKO-FITNESS** on GitHub. Nothing from any other project (e.g. clearline) is included.

---

## Step 1: Create the Eko-only folder

In **Terminal**, run:

```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT
chmod +x prepare-eko-for-github.sh
./prepare-eko-for-github.sh
```

This creates a folder **inside DLT** called **`EKO-FITNESS-app`** containing only:

- **eko-react/** (frontend)
- **api/routes/football.py** (backend API)
- **core/** (database)
- **main.py** (Eko-only server)
- **requirements.txt**, **.gitignore**

---

## Step 2: Go into that folder

```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT/EKO-FITNESS-app
```

(If your DLT folder is elsewhere, use that path instead, e.g. `~/Downloads/DLT/EKO-FITNESS-app`.)

---

## Step 3: Start a new git repo and push to EKO-FITNESS

Run these one by one:

```bash
git init
git add .
git commit -m "Eko Football app"
git remote add origin https://github.com/timkene/EKO-FITNESS.git
git branch -M main
git push -u origin main
```

If you use SSH for GitHub instead of HTTPS:

```bash
git remote add origin git@github.com:timkene/EKO-FITNESS.git
```

then run `git push -u origin main` as above.

---

## Step 4: Confirm on GitHub

Open: **https://github.com/timkene/EKO-FITNESS**

You should see only the Eko app (eko-react, backend, no other project).

---

## Summary

| Step | What you do |
|------|-------------|
| 1 | From `DLT`, run `./prepare-eko-for-github.sh` |
| 2 | `cd` into `DLT/EKO-FITNESS-app` |
| 3 | `git init` → `git add .` → `git commit -m "Eko Football app"` |
| 4 | `git remote add origin https://github.com/timkene/EKO-FITNESS.git` |
| 5 | `git branch -M main` → `git push -u origin main` |

After this, **EKO-FITNESS** on GitHub contains only the Eko React app, with no other project and no branch named after anything else.
