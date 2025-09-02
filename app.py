import os, json
from fastapi import FastAPI, Request, HTTPException
import aiofiles, httpx

APP_SECRET = os.getenv("ALPHAOPS_SECRET", "")
DISCORD    = os.getenv("DISCORD_WEBHOOK_STATUS", "")
LOG_PATH   = os.getenv("LOG_PATH", "/mnt/data/cme_sandbox.jsonl")

# Ensure the directory exists at startup
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

app = FastAPI(title="AlphaOps CME Sandbox")

@app.post("/ingest/test")
async def ingest_tv(request: Request):
    body = await request.body()
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON")

    if payload.get("auth") != APP_SECRET:
        raise HTTPException(status_code=401, detail="bad secret")

    # Append to log file safely
    async with aiofiles.open(LOG_PATH, "a") as f:
        await f.write(json.dumps(payload) + "\n")

    if DISCORD:
        summary = f"✅ {payload.get('exchange')} {payload.get('symbol')} {payload.get('tf')} | split_abs={payload.get('rsi_split_abs')} | d_bps={payload.get('dist_d_bps')}"
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(DISCORD, json={"content": summary})

    return {"ok": True}
