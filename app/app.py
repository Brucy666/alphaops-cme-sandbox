import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException
import aiofiles
import httpx
from supabase import create_client, Client

# ---------------- env ----------------
APP_SECRET = os.getenv("ALPHAOPS_SECRET", "")
RELAXED_AUTH = os.getenv("RELAXED_AUTH", "0").lower() in ("1", "true", "yes")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_STATUS", "")
LOG_PATH = os.getenv("LOG_PATH", "/mnt/data/cme_sandbox.json")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# ---------------- app & supabase ----------------
app = FastAPI(title="AlphaOps CME RSI Bot (Compat)")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------- helpers ----------
def _utc_iso(ts: Optional[str] = None) -> str:
    if ts:
        # Accept either ISO or TradingView's {{timenow}} string
        try:
            # Try native parse from ISO-ish; if it fails we'll fallback to now
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


async def _save_line(obj: Dict[str, Any]) -> None:
    try:
        async with aiofiles.open(LOG_PATH, "a") as f:
            await f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _maybe_json(s: str) -> Optional[Dict[str, Any]]:
    s = s.strip()
    if not s:
        return None
    # JSON body
    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except Exception:
            return None
    # key:value list  e.g.  "symbol:BTC, rsi:58.2, tf:1m"
    if ":" in s and "," in s:
        try:
            out: Dict[str, Any] = {}
            for pair in s.split(","):
                k, v = pair.split(":", 1)
                out[k.strip()] = v.strip()
            return out
        except Exception:
            return None
    return None


def _coerce_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _pick_first(d: Dict[str, Any], keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _extract_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map many possible field names (old/new) into a standard row for hp_cme_rsi:
      ts, symbol, tf, rsi, dist_bps, exchange, source, payload
    """
    # standardize keys: allow both old short names and new verbose
    symbol = _pick_first(raw, ["symbol", "s", "ticker"], "BTCUSD.P")
    tf     = _pick_first(raw, ["tf", "interval", "i"], "1m")
    rsi    = _coerce_float(_pick_first(raw, ["rsi", "r"], 0.0))
    dist   = _coerce_float(_pick_first(raw, ["dist_d_bps", "dist_bps", "d_bps"], 0.0))
    exch   = _pick_first(raw, ["exchange", "ex"], "CME")
    ts_in  = _pick_first(raw, ["timestamp", "ts", "timenow", "time"], None)

    row = {
        "ts": _utc_iso(ts_in),
        "symbol": str(symbol),
        "tf": str(tf),
        "rsi": rsi,
        "dist_bps": dist,
        "exchange": str(exch),
        "source": str(_pick_first(raw, ["source", "src"], "tradingview")),
        "payload": raw,  # keep the original for forensics/debug
    }
    return row


def _auth_ok(raw: Dict[str, Any]) -> bool:
    # Allow relaxed mode for today if env is set.
    if RELAXED_AUTH:
        return True
    if not APP_SECRET:
        # No secret configured -> accept to avoid accidental lockout
        return True
    return raw.get("auth") == APP_SECRET


async def _discord_post(text: str):
    if not DISCORD_WEBHOOK:
        return
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(DISCORD_WEBHOOK, json={"content": text})
    except Exception:
        pass


# ---------- routes ----------
@app.get("/health")
async def health():
    return {"ok": True, "ts": _utc_iso()}


@app.post("/ingest/test")
async def ingest(request: Request):
    """
    Back-compatible ingestion:
    - Accepts JSON, form, or raw text.
    - Auth:
        * if RELAXED_AUTH=1 -> auth optional
        * else, require body.auth == ALPHAOPS_SECRET
    - Writes to hp_cme_rsi; fills defaults if missing.
    """
    # 1) Try to get body in all the common ways TradingView might send it
    body_bytes = await request.body()
    body_text = body_bytes.decode("utf-8", errors="ignore").strip()
    data: Optional[Dict[str, Any]] = None

    # Prefer JSON if we can
    data = _maybe_json(body_text)

    # Try form as a backup (some webhooks send "payload" field)
    if data is None:
        try:
            form = await request.form()
            if "payload" in form:
                maybe = _maybe_json(str(form["payload"]))
                if maybe is not None:
                    data = maybe
            # Some integrations put text directly in a "message" field
            if data is None and "message" in form:
                maybe = _maybe_json(str(form["message"]))
                if maybe is not None:
                    data = maybe
        except Exception:
            pass

    # If still nothing, create a minimal container with just the raw text
    if data is None:
        data = {"raw": body_text}

    # 2) Auth check (relaxed allowed)
    if not _auth_ok(data):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 3) Normalize and upsert
    row = _extract_payload(data)
    await _save_line({"ingest": row})

    # Supabase insert (skip gracefully if not configured)
    if supabase is not None:
        try:
            supabase.table("hp_cme_rsi").upsert(row).execute()
        except Exception as e:
            # Still return ok; we logged to file already
            await _save_line({"supabase_error": str(e)})

    # 4) Discord (optional)
    await _discord_post(
        f"📊 **CME RSI**  {row['symbol']}  |  TF `{row['tf']}`  |  RSI `{row['rsi']}`  |  Δbps `{row['dist_bps']}`"
    )

    return {"ok": True, "ts": row["ts"]}
