# app.py — AlphaOps CME Sandbox (Extended RSI + CVD)
import os, json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, HTTPException
import aiofiles, httpx
from supabase import create_client, Client

# ───────────────────────────────────────────────
# Environment
APP_SECRET = os.getenv("ALPHAOPS_SECRET", "")
RELAXED_AUTH = os.getenv("RELAXED_AUTH", "0").lower() in ("1","true","yes")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_STATUS", "")
LOG_PATH = os.getenv("LOG_PATH", "/mnt/data/cme_sandbox.json")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

app = FastAPI(title="AlphaOps CME RSI + CVD Feed")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ───────────────────────────────────────────────
# Helpers
def utc_iso(ts: Optional[str] = None) -> str:
    if ts:
        try:
            return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc).isoformat()
        except: pass
    return datetime.now(timezone.utc).isoformat()

async def save_line(obj: Dict[str, Any]):
    async with aiofiles.open(LOG_PATH, "a") as f:
        await f.write(json.dumps(obj, ensure_ascii=False)+"\n")

async def discord_post(msg: str):
    if not DISCORD_WEBHOOK: return
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(DISCORD_WEBHOOK, json={"content": msg})
    except: pass

def coerce_float(x): 
    try: return float(x)
    except: return 0.0

def auth_ok(data: Dict[str, Any]) -> bool:
    if RELAXED_AUTH: return True
    if not APP_SECRET: return True
    return str(data.get("sec")) == APP_SECRET or str(data.get("auth")) == APP_SECRET

# ───────────────────────────────────────────────
# Routes

@app.get("/health")
async def health(): return {"ok": True, "ts": utc_iso()}

@app.post("/ingest/test")
async def ingest_rsi(request: Request):
    body = await request.body()
    data = json.loads(body.decode("utf-8","ignore"))
    if not auth_ok(data): raise HTTPException(status_code=401, detail="Unauthorized")

    row = {
        "ts": utc_iso(data.get("time")),
        "symbol": data.get("symbol", "BTCUSD.P"),
        "tf": data.get("tf", "1m"),
        "rsi": coerce_float(data.get("rsi")),
        "dist_bps": coerce_float(data.get("dist_bps")),
        "exchange": data.get("ex", "CME"),
        "source": "tradingview",
        "payload": data
    }
    await save_line({"rsi_ingest": row})
    if supabase: supabase.table("hp_cme_rsi").upsert(row).execute()
    await discord_post(f"📊 **CME RSI** {row['symbol']} | TF {row['tf']} | RSI {row['rsi']:.1f}")
    return {"ok": True, "type": "RSI", "ts": row["ts"]}

@app.post("/ingest/cvd")
async def ingest_cvd(request: Request):
    body = await request.body()
    data = json.loads(body.decode("utf-8","ignore"))
    if not auth_ok(data): raise HTTPException(status_code=401, detail="Unauthorized")

    row = {
        "ts": utc_iso(data.get("time")),
        "symbol": data.get("symbol", "BTC1!"),
        "tf": data.get("tf", "12m"),
        "price": coerce_float(data.get("price")),
        "volume": coerce_float(data.get("volume")),
        "delta": coerce_float(data.get("delta")),
        "cvd": coerce_float(data.get("cvd")),
        "exchange": data.get("ex", "CME"),
        "source": "tradingview",
        "payload": data
    }
    await save_line({"cvd_ingest": row})
    if supabase: supabase.table("hp_cme_cvd").upsert(row).execute()
    await discord_post(f"🧭 **CME CVD** {row['symbol']} | TF {row['tf']} | Δ {row['delta']:.2f} | CVD {row['cvd']:.2f}")
    return {"ok": True, "type": "CVD", "ts": row["ts"]}
