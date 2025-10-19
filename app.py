# app.py
import os, json
from fastapi import FastAPI, Request, HTTPException
import aiofiles, httpx

APP_SECRET = os.getenv("ALPHAOPS_SECRET", "")
DISCORD    = os.getenv("DISCORD_WEBHOOK_STATUS", "")
LOG_PATH   = os.getenv("LOG_PATH", "/mnt/data/cme_sandbox.jsonl")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
CME_TABLE    = os.getenv("SUPABASE_CME_TABLE", "hp_cme_rsi")

app = FastAPI(title="AlphaOps CME Sandbox")

async def upsert_cme_to_supabase(payload: dict):
    """Upsert CME RSI event into Supabase."""
    if not (SUPABASE_URL and SUPABASE_KEY):
        return  # quietly skip if not configured

    # Map/normalize incoming payload → table columns
    # From your code: exchange, symbol, t, rsi_split_abs, dist_d_bps, plus tf maybe
    ts_raw   = payload.get("t")                       # expect ISO string from TV
    symbol   = payload.get("symbol") or "BTCUSD.P"
    tf       = payload.get("tf") or payload.get("timeframe") or "1m"
    rsi_val  = payload.get("rsi_split_abs") or payload.get("rsi")
    dist_bps = payload.get("dist_d_bps") or payload.get("d_bps")

    row = {
        "ts":       ts_raw,
        "symbol":   str(symbol),
        "tf":       str(tf),
        "exchange": payload.get("exchange") or "CME",
        "rsi":      float(rsi_val) if rsi_val is not None else None,
        "dist_bps": float(dist_bps) if dist_bps is not None else None,
        "payload":  payload,
    }

    # Supabase PostgREST upsert (merge duplicates by unique index)
    url = f"{SUPABASE_URL}/rest/v1/{CME_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.post(url, headers=headers, json=[row])
        resp.raise_for_status()

@app.post("/ingest/test")
async def ingest_tv(request: Request):
    body = await request.body()
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON")

    if payload.get("auth") != APP_SECRET:
        raise HTTPException(status_code=401, detail="bad secret")

    # Log to file (unchanged)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    async with aiofiles.open(LOG_PATH, "a") as f:
        await f.write(json.dumps(payload) + "\n")

    # Upsert to Supabase (NEW)
    try:
        await upsert_cme_to_supabase(payload)
    except Exception as e:
       
