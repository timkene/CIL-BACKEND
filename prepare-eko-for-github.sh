#!/bin/bash
# Prepares a folder with ONLY the Eko Football app (no other project).
# Run from inside DLT:  ./prepare-eko-for-github.sh
# Then follow PUSH_EKO_TO_GITHUB.md to push to your EKO-FITNESS repo.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$SCRIPT_DIR/EKO-FITNESS-app"
echo "Creating $OUT with Eko app only..."

rm -rf "$OUT"
mkdir -p "$OUT/api/routes"
mkdir -p "$OUT/core"
mkdir -p "$OUT/uploads/football"

cp -R "$SCRIPT_DIR/eko-react" "$OUT/"
cp "$SCRIPT_DIR/api/routes/football.py" "$OUT/api/routes/"
cp "$SCRIPT_DIR/core/database.py" "$OUT/core/"
cp "$SCRIPT_DIR/core/__init__.py" "$OUT/core/" 2>/dev/null || true
cp "$SCRIPT_DIR/main_eko.py" "$OUT/main.py"
cp "$SCRIPT_DIR/requirements.txt" "$OUT/"
cp "$SCRIPT_DIR/DEPLOY_RENDER.md" "$OUT/" 2>/dev/null || true
touch "$OUT/api/__init__.py"
touch "$OUT/api/routes/__init__.py"

# .gitignore for Eko-only repo
cat > "$OUT/.gitignore" << 'GITIGNORE'
.env
.env.*
!.env.example
node_modules/
__pycache__/
*.py[cod]
*.duckdb
*.db
.DS_Store
GITIGNORE

echo "Done. Next (run these in Terminal):"
echo "  cd $OUT"
echo "  git init"
echo "  git add ."
echo "  git commit -m 'Eko Football app'"
echo "  git remote add origin https://github.com/timkene/EKO-FITNESS.git"
echo "  git branch -M main"
echo "  git push -u origin main"
echo ""
echo "Or see PUSH_EKO_TO_GITHUB.md for full steps."
