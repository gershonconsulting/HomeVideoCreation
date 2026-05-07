#!/usr/bin/env bash
# Installs the system tools Souvenir needs inside the Codespace,
# then prepares the Node project. Re-run anytime if something goes sideways.
set -euo pipefail

echo "──────────────────────────────────────────────"
echo "  Souvenir · Codespace setup"
echo "──────────────────────────────────────────────"

echo ""
echo "→ Installing ffmpeg + serif fonts via apt..."
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  ffmpeg \
  fonts-liberation \
  fonts-dejavu \
  ca-certificates

echo ""
echo "→ Installing yt-dlp via pip..."
# Use the user-scoped pip so we don't fight with system Python ownership.
python3 -m pip install --user --upgrade --quiet yt-dlp

# Make sure ~/.local/bin is on PATH for future shells (Codespaces inherits this)
if ! grep -q '\.local/bin' "$HOME/.bashrc" 2>/dev/null; then
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
fi
export PATH="$HOME/.local/bin:$PATH"

echo ""
echo "→ Installing npm dependencies..."
npm install --no-audit --no-fund

echo ""
echo "→ Verifying tools..."
node --version | sed 's/^/  node:    /'
ffmpeg -version 2>&1 | head -1 | sed 's/^/  ffmpeg:  /'
yt-dlp --version 2>&1 | sed 's/^/  yt-dlp:  /'

echo ""
echo "✓ Setup complete."
echo ""
echo "  Start the server with:    npm start"
echo "  Then open the forwarded port (3737) — Codespaces will pop a notification."
echo ""
