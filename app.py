import os
import json
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
import aiofiles
import httpx
from supabase import create_client, Client

# === ENVIRONMENT VARIABLES ===
APP_SECRET = os.getenv("ALPHAOPS_SECRET", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_STATUS", "")
LOG_PATH = os.getenv("LOG_PATH", "/mnt/data/cme_sandbox.json")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# === INITIALIZE ===
app = FastAPI(title="AlphaOps CME RSI Bot")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# === SUPABASE CLIENT ===
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


@app.post("/ingest/test")
async def ingest(request: Request):
    try:
        body = await request.body()
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # === SECURITY CHECK ===
    if payload.get("auth") != APP_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # === SAVE TO LOG ===
    async with aiofiles.open(LOG_PATH, "a") as f:
        await f.write(json.dumps(payload) + "\n")

    # === UPSERT INTO SUPABASE ===
    try:
        ts = payload.get("timestamp") or datetime.utcnow().isoformat()
        symbol = payload.get("symbol", "BTCUSD.P")
        rsi = float(payload.get("rsi", 0))
        dist_bps = float(payload.get("dist_d_bps", 0))
        tf = payload.get("tf", "1m")

        data = {
            "ts": ts,
            "symbol": symbol,
            "tf": tf,
            "exchange": "CME",
            "rsi": rsi,
            "dist_bps": dist_bps,
            "payload": payload,
        }

        result = supabase.table("hp_cme_rsi").upsert(data).execute()
        print(f"Inserted CME RSI for {symbol} at {ts}")

    except Exception as e:
        print(f"Supabase insert error: {e}")

    # === DISCORD POST (optional) ===
    if DISCORD_WEBHOOK:
        try:
            summary = (
                f"📊 **CME RSI Update** — {symbol}\n"
                f"RSI: `{rsi}` | Δbps: `{dist_bps}` | TF: `{tf}`"
            )
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(DISCORD_WEBHOOK, json={"content": summary})
        except Exception as e:
            print(f"Discord error: {e}")

    return {"status": "ok"}


@app.get("/")
async def root():
    return {"status": "CME RSI bot running", "time": datetime.utcnow().isoformat()}
