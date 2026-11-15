#!/usr/bin/env bash
set -euo pipefail

echo "Use this as a checklist; it does not install software or modify Docker settings."
echo "1. Keep the checkout in the WSL2 Linux filesystem (for example ~/finstream)."
echo "2. Install Python 3.11, Docker Desktop WSL2 integration, Git, and Docker Compose V2."
echo "3. Copy .env.example to .env and replace local placeholder secrets."
echo "4. Run make validate, then start one Compose profile at a time."
