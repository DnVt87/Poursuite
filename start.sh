#!/usr/bin/env bash
# start.sh — launch the Poursuite API + a Cloudflare tunnel with one command.
#
#   bash start.sh
#
# Runs both in THIS window; Ctrl-C stops both cleanly. The public URL is printed
# once the tunnel is up. Live logs from both are tailed below.
#
# API key (required), resolved in this order:
#   1. $POURSUITE_API_KEY if already set in the environment
#   2. a local file  .poursuite_api_key  (gitignored — create it once:
#         echo "your-secret-key" > .poursuite_api_key )
#   3. an interactive prompt
#
# Tunnel: defaults to a Cloudflare *quick* tunnel (random *.trycloudflare.com
# URL — no account/login needed). To use a named tunnel instead (once one is
# set up for app.poursuite.com.br), run:  TUNNEL_NAME=poursuite bash start.sh
#
# Other overrides: POURSUITE_PORT (default 8000), POURSUITE_LOG_DIR.

cd "$(dirname "$0")" || exit 1

# --- Python interpreter (the repo's venv) ----------------------------------
PY=".venv/Scripts/python.exe"            # Windows
[ -x "$PY" ] || PY=".venv/bin/python"    # macOS/Linux fallback
if [ ! -x "$PY" ]; then
  echo "ERROR: venv Python not found. Expected .venv/Scripts/python.exe"
  echo "       Set one up:  python -m venv .venv && .venv/Scripts/python.exe -m pip install -e ."
  exit 1
fi

# --- API key ----------------------------------------------------------------
if [ -z "${POURSUITE_API_KEY:-}" ]; then
  if [ -f ".poursuite_api_key" ]; then
    POURSUITE_API_KEY="$(tr -d '\r\n' < .poursuite_api_key)"
  else
    printf "POURSUITE_API_KEY (input hidden): "
    read -rs POURSUITE_API_KEY
    printf "\n"
  fi
fi
if [ -z "${POURSUITE_API_KEY:-}" ]; then
  echo "ERROR: POURSUITE_API_KEY is empty. Set it, or create .poursuite_api_key."
  exit 1
fi
export POURSUITE_API_KEY

# --- Config -----------------------------------------------------------------
PORT="${POURSUITE_PORT:-8000}"
TUNNEL_NAME="${TUNNEL_NAME:-}"           # empty = quick tunnel
LOGDIR="${POURSUITE_LOG_DIR:-C:/Poursuite/Logs}"
mkdir -p "$LOGDIR" 2>/dev/null || LOGDIR="."
APP_LOG="$LOGDIR/start_app.log"
CF_LOG="$LOGDIR/start_cloudflared.log"
: > "$APP_LOG"; : > "$CF_LOG"

# --- Cleanup on Ctrl-C / exit ----------------------------------------------
APP_PID=""; CF_PID=""
cleanup() {
  echo ""
  echo "Stopping…"
  [ -n "$CF_PID" ]  && kill "$CF_PID"  2>/dev/null
  [ -n "$APP_PID" ] && kill "$APP_PID" 2>/dev/null
  wait 2>/dev/null
  echo "Stopped."
}
trap 'cleanup; exit 0' INT TERM

# --- Start the API ----------------------------------------------------------
echo "▶ Starting API on http://localhost:$PORT  (log: $APP_LOG)"
"$PY" -m uvicorn poursuite.api.main:app --host 0.0.0.0 --port "$PORT" --workers 1 \
  >"$APP_LOG" 2>&1 &
APP_PID=$!

# Wait for the API to answer before bringing up the tunnel.
printf "  waiting for API"
for _ in $(seq 1 30); do
  if "$PY" -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:$PORT/',timeout=2)" 2>/dev/null; then
    printf " — up\n"; break
  fi
  if ! kill -0 "$APP_PID" 2>/dev/null; then
    printf " — FAILED\n"; echo "API process died. Last log lines:"; tail -n 20 "$APP_LOG"; cleanup; exit 1
  fi
  printf "."; sleep 1
done

# --- Start the Cloudflare tunnel -------------------------------------------
if ! command -v cloudflared >/dev/null 2>&1; then
  echo "⚠ cloudflared not found — API is reachable locally only at http://localhost:$PORT"
else
  if [ -n "$TUNNEL_NAME" ]; then
    echo "▶ Starting named Cloudflare tunnel '$TUNNEL_NAME'  (log: $CF_LOG)"
    cloudflared tunnel run "$TUNNEL_NAME" >"$CF_LOG" 2>&1 &
    CF_PID=$!
  else
    echo "▶ Starting Cloudflare quick tunnel  (log: $CF_LOG)"
    cloudflared tunnel --url "http://localhost:$PORT" >"$CF_LOG" 2>&1 &
    CF_PID=$!
    printf "  waiting for tunnel URL"
    for _ in $(seq 1 30); do
      URL="$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$CF_LOG" 2>/dev/null | head -1)"
      [ -n "$URL" ] && break
      printf "."; sleep 1
    done
    printf "\n"
    if [ -z "${URL:-}" ]; then
      echo "  (URL not captured yet — check $CF_LOG)"
    fi
  fi
fi

echo ""
echo "═══════════════════════════════════════════════════════════════════════════"
if [ -n "${URL:-}" ]; then
  echo "   OPEN THIS URL IN A BROWSER (and paste your API key in the page header):"
  echo ""
  echo "        $URL"
  echo ""
  echo "   The URL changes every run — re-send it to the lawyers each time."
elif [ -n "$TUNNEL_NAME" ]; then
  echo "   Named tunnel '$TUNNEL_NAME' is up — use your configured hostname."
else
  echo "   Tunnel URL not captured — check $CF_LOG"
fi
echo "═══════════════════════════════════════════════════════════════════════════"
echo ""
echo "   RUNNING. Keep this window open. Press Ctrl-C to stop both."
echo "   Logs (if you need them):  $APP_LOG"
echo "                             $CF_LOG"

# Wait quietly until Ctrl-C (trap) or a process exits — no log flood, so the
# URL above stays visible. Logs are still written to the files above.
wait
cleanup
