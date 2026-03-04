#!/bin/bash
# Push current Eko Football code to GitHub repo EKO-FITNESS (main branch)
# Run from this directory: ./push-to-eko-fitness.sh

set -e
cd "$(dirname "$0")"
echo "Pushing to EKO-FITNESS (eko remote, main branch)..."
git push eko clearline-portal:main
echo "Done. Check https://github.com/timkene/EKO-FITNESS"
