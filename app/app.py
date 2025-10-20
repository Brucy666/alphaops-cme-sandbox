# app.py
import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Any, Dict

import aiofiles
import httpx
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator

# Supabase v2
try:
    from supabase import create_client, Client  # type: ignore
except Exception:  # pragma: no cover
    create_client = None  # will be validated on first use
    Client = Any

# ------------------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------------------
APP_SECRET = os.getenv("ALPHAOPS_SECRET", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_STATUS", "")
LOG_PATH = os.getenv("LOG_PATH", "/mnt/data/cme_sandbox.json")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

TABLE_NAME = os.getenv("CME_RSI_TABLE", "hp_cme_rsi")
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "BTCUSD.P")
DEFAULT_TF = os.getenv("DEFAULT_TF", "1m")

APP_VERSION = os.getenv("APP_VERSION", "2025-10-20.1")

# make sure the log folder exists
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ | %(levelname)s | %(message)s",
)
logger = logging.getLogger("cme-rsi-bot")


# ------------------------------------------------------------------------------
# FastAPI
# ------------------------------------------------------------------------------
app = FastAPI(title="AlphaOps CME RSI Bot", version=APP_VERSION)

# allow internal tools / dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------------------
# Pydantic payload model (what TradingView / your CME adapter will POST)
# ------------------------------------------------------------------------------
class CmeRsiPayload(BaseModel):
    # auth can be in body or in X-Alphaops-Auth header
    auth: Optional[str] = None

    # business fields
    timestamp: Optional[str] = None  # ISO string, we also accept epoch via 'ts'
    ts: Optional[str] = None  # alias accepted
    symbol: str = Field(default=DEFAULT_SYMBOL)
    tf: str = Field(default=DEFAULT_TF)
    rsi: float = 0.0
    dist_d_bps: Optional[float] = Field(default=None)
    dist_bps: Optional[float] = Field(default=None)  # also accept this alias
    exchange: str = Field(default="CME")

    # keep whole original payload too (for debug)
    extra: Optional[Dict[str, Any]] = None

    @validator("timestamp", pre=True, always=True)
    def normalize_timestamp(cls, v, values):
        # allow fallback to 'ts' and default to now
        ts = v or values.get("ts")
        if not ts:
            # now in UTC, isoformat with Z
            return datetime.now(timezone.utc).isoformat()
        return str(ts)

    @validator("dist_bps", always=True)
    def coalesce_dist_bps(cls, v, values):
        # prefer explicit dist_bps; else fall back to dist_d_bps; else 0
        if v is not None:
            return float(v)
        d = values.get("dist_d_bps")
        return float(d) if d is not None else 0.0


# ------------------------------------------------------------------------------
# Supabase client (lazy so env issues are reported clearly)
# ------------------------------------------------------------------------------
_sb: Optional[Client] = None


def get_supabase() -> Client:
    global _sb
    if _sb is not None:
        return _sb
    if not create_client:
        raise RuntimeError("supabase package not available. Check requirements.txt")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.")
    _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
async def log_line(obj: Dict[str, Any]) -> None:
    """Append a compact line JSON to LOG_PATH (fire-and-forget for troubleshooting)."""
    try:
        async with aiofiles.open(LOG_PATH, "a") as f:
            await f.write(json.dumps(obj, separators=(",", ":")) + "\n")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Log write error: {e}")


async def send_discord(content: str) -> None:
    if not DISCORD_WEBHOOK:
        return
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(DISCORD_WEBHOOK, json={"content": content})
    except Exception as e:  # pragma: no cover
        logger.warning(f"Discord error: {e}")


def ok() -> Dict[str, str]:
    return {"status": "ok"}


# ------------------------------------------------------------------------------
# Health / meta endpoints
# ------------------------------------------------------------------------------
@app.get("/")
async def root():
    # minimal info so we know env is loading
    return {
        "status": "CME RSI bot running",
        "version": APP_VERSION,
        "supabase_url_set": bool(SUPABASE_URL),
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/_/ready")
async def ready():
    # verify supabase is usable
    try:
        sb = get_supabase()
        # cheap ping: no query, just ensure client object exists
        _ = sb.storage  # attribute touch
        return ok()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"supabase not ready: {e}")


@app.get("/_/health")
async def health():
    return ok()


# ------------------------------------------------------------------------------
# Ingest endpoints
# ------------------------------------------------------------------------------
@app.post("/ingest/test")
@app.post("/ingest")  # allow both
async def ingest(request: Request, x_alphaops_auth: Optional[str] = Header(default=None)):
    """
    Accepts CME RSI ticks. POST JSON like:

    {
      "auth": "...",                   # or send in header X-Alphaops-Auth
      "timestamp": "2025-10-20T07:40:00Z",
      "symbol": "BTCUSD.P",
      "tf": "1m",
      "rsi": 58.2,
      "dist_d_bps": 12.7
    }
    """
    # --- decode & validate JSON ---
    try:
        body_bytes = await request.body()
        raw = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    payload = CmeRsiPayload(**raw)

    # --- auth (header overrides body if present) ---
    token = (x_alphaops_auth or payload.auth or "").strip()
    if not APP_SECRET or token != APP_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # --- compose the row ---
    row = {
        "ts": payload.timestamp,         # timestamptz
        "symbol": payload.symbol,        # text
        "tf": payload.tf,                # text
        "exchange": payload.exchange,    # text ("CME")
        "rsi": float(payload.rsi),       # numeric
        "dist_bps": float(payload.dist_bps),  # numeric
        "payload": raw,                  # jsonb (original request)
    }

    # --- log line for forensic/debug ---
    await log_line({"ingest": row})

    # --- upsert to Supabase ---
    try:
        sb = get_supabase()
        # primary key can be (symbol, ts) or an autoincrement id; upsert tolerates either
        res = sb.table(TABLE_NAME).upsert(row).execute()
        logger.info(f"Upserted CME RSI: {payload.symbol} @ {payload.timestamp} ({payload.tf})")
    except Exception as e:
        logger.error(f"Supabase upsert failed: {e}")
        # still return 202 so TradingView doesn't retry endlessly
        await send_discord(f"⚠️ CME RSI upsert failed: `{e}`")
        return {"status": "accepted", "error": str(e)}

    # --- optional status message ---
    await send_discord(
        f"📊 **CME RSI** — {payload.symbol} | tf `{payload.tf}` | "
        f"RSI `{payload.rsi:.2f}` | Δbps `{payload.dist_bps:.2f}`"
    )

    return ok()


# ------------------------------------------------------------------------------
# Run local (optional)
# ------------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
