# -*- coding: utf-8 -*-
"""
AlphaOps CME Webhook Sandbox (FastAPI)
- /health
- /ingest/test      -> alias for RSI (back-compat)
- /ingest/cme_rsi   -> RSI/dist_bps
- /ingest/cme_cvd   -> CVD/Δ/volume
"""
import os, json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException
import aiofiles, httpx

try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None

# ---------- env ----------
APP_SECRET   = os.getenv("ALPHAOPS_SECRET", "")
RELAXED_AUTH = os.getenv("RELAXED_AUTH","0").lower() in ("1","true","yes")

DISCORD_WEBHOOK_STATUS = os.getenv("DISCORD_WEBHOOK_STATUS","")
DISCORD_WEBHOOK_CME    = os.getenv("DISCORD_WEBHOOK_CME","")  # optional ping for big Δ

LOG_PATH = os.getenv("LOG_PATH","/mnt/data/cme_sandbox.json")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL","")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY","")

sb: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY and create_client:
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print("Supabase init failed:", e)

app = FastAPI(title="AlphaOps CME Webhook Sandbox")

# ---------- helpers ----------
def utc_iso(ts: Optional[str]=None) -> str:
    if ts:
        try:
            return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()

async def save_line(obj: Dict[str,Any]) -> None:
    try:
        async with aiofiles.open(LOG_PATH,"a") as f:
            await f.write(json.dumps(obj, ensure_ascii=False)+"\n")
    except Exception:
        pass

async def post_discord(url:str, text:str):
    if not url: return
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            await c.post(url, json={"content": text})
    except Exception:
        pass

def maybe_json(txt: str) -> Optional[Dict[str,Any]]:
    t = (txt or "").strip()
    if not t: return None
    if t.startswith("{") and t.endswith("}"):
        try: return json.loads(t)
        except Exception: return None
    if ":" in t and "," in t:   # "k:v, k2:v2"
        try:
            out: Dict[str,Any] = {}
            for p in t.split(","):
                k,v = p.split(":",1)
                out[k.strip()] = v.strip()
            return out
        except Exception:
            return None
    return None

async def parse_body(req: Request) -> Dict[str,Any]:
    # JSON first
    try: return await req.json()
    except Exception: pass
    # form payload/message
    try:
        form = await req.form()
        for key in ("payload","message"):
            if key in form:
                mj = maybe_json(str(form[key]))
                if mj is not None: return mj
    except Exception: pass
    # raw text
    raw = (await req.body()).decode("utf-8", errors="ignore")
    mj = maybe_json(raw)
    return mj if mj is not None else {"raw": (raw or "").strip()}

def auth_ok(d: Dict[str,Any]) -> bool:
    if RELAXED_AUTH: return True
    if not APP_SECRET: return True
    return d.get("auth")==APP_SECRET or d.get("sec")==APP_SECRET

def pick(d: Dict[str,Any], keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None,""): return d[k]
    return default

def fnum(x: Any, default: float=0.0) -> float:
    try:
        if x is None: return default
        if isinstance(x,(int,float)): return float(x)
        s=str(x).strip()
        return default if s=="" else float(s)
    except Exception:
        return default

# ---------- mappers ----------
def map_rsi(d: Dict[str,Any]) -> Dict[str,Any]:
    symbol = pick(d,["symbol","s","ticker"],"BTC1!")
    tf     = pick(d,["tf","interval","i"],"1m")
    rsi    = fnum(pick(d,["rsi","r"],0.0))
    dist   = fnum(pick(d,["dist_d_bps","dist_bps","d_bps"],0.0))
    exch   = pick(d,["exchange","ex"],"CME")
    ts_in  = pick(d,["timestamp","ts","timenow","time"],None)
    ts_iso = utc_iso(ts_in)
    return {
        "ts": ts_iso, "symbol": str(symbol), "tf": str(tf),
        "rsi": rsi, "dist_bps": dist, "exchange": str(exch),
        "source": str(pick(d,["source","src"],"tradingview")),
        "payload": d, "natural_key": f"{symbol}:{tf}:{ts_iso}"
    }

def map_cvd(d: Dict[str,Any]) -> Dict[str,Any]:
    symbol = pick(d,["symbol","s","ticker"],"BTC1!")
    tf     = pick(d,["tf","interval","i"],"1m")
    price  = fnum(pick(d,["price","p"],0.0))
    vol    = fnum(pick(d,["volume","v"],0.0))
    delta  = fnum(pick(d,["delta","d","Δ"],0.0))
    cvd    = fnum(pick(d,["cvd","cum_delta","cum"],0.0))
    ts_in  = pick(d,["timestamp","ts","timenow","time"],None)
    ts_iso = utc_iso(ts_in)
    return {
        "ts": ts_iso, "symbol": str(symbol), "tf": str(tf),
        "price": price, "volume": vol, "delta": delta, "cvd": cvd,
        "src": str(pick(d,["source","src"],"tradingview")),
        "payload": d, "natural_key": f"{symbol}:{tf}:{ts_iso}"
    }

# ---------- routes ----------
@app.get("/health")
async def health(): return {"ok": True, "ts": utc_iso()}

@app.post("/ingest/cme_rsi")
async def ingest_cme_rsi(req: Request):
    body = await parse_body(req)
    if not auth_ok(body): raise HTTPException(status_code=401, detail="Unauthorized")
    row = map_rsi(body)
    await save_line({"cme_rsi": row})
    if sb:
        try: sb.table("hp_cme_rsi").upsert(row, on_conflict="natural_key").execute()
        except Exception as e: await save_line({"sb_err_rsi": str(e)})
    if DISCORD_WEBHOOK_STATUS:
        await post_discord(DISCORD_WEBHOOK_STATUS,
            f"📊 **CME RSI** {row['symbol']} tf `{row['tf']}` rsi `{row['rsi']:.1f}` Δbps `{row['dist_bps']:.0f}` @ {row['ts']}")
    return {"ok": True, "ts": row["ts"]}

# back-compat alias to keep existing alerts alive
@app.post("/ingest/test")
async def ingest_test(req: Request): return await ingest_cme_rsi(req)

@app.post("/ingest/cme_cvd")
async def ingest_cme_cvd(req: Request):
    body = await parse_body(req)
    if not auth_ok(body): raise HTTPException(status_code=401, detail="Unauthorized")
    row = map_cvd(body)
    await save_line({"cme_cvd": row})
    if sb:
        try: sb.table("hp_cme_cvd").upsert(row, on_conflict="natural_key").execute()
        except Exception as e: await save_line({"sb_err_cvd": str(e)})

    # optional ping for big deltas
    if abs(row.get("delta",0)) >= 1500 and DISCORD_WEBHOOK_CME:
        await post_discord(DISCORD_WEBHOOK_CME,
            f"🧭 **CME Δ** {row['symbol']} tf `{row['tf']}` Δ `{row['delta']:.0f}` CVD `{row['cvd']:.0f}` px `{row['price']:.0f}`")
    return {"ok": True, "ts": row["ts"]}
