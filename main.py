
import asyncio
import json
from datetime import datetime, timezone

import websockets
from fastapi import FastAPI

app = FastAPI(title="Nova XRP Live Bridge")

prices = {}
connection_status = "Starting"

PAIRS = ["XRP/USD", "XRP/CAD"]
KRAKEN_WS = "wss://ws.kraken.com"


async def kraken_feed():
    global connection_status

    while True:
        try:
            connection_status = "Connecting"

            async with websockets.connect(
                KRAKEN_WS,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                await ws.send(json.dumps({
                    "event": "subscribe",
                    "pair": PAIRS,
                    "subscription": {"name": "ticker"}
                }))

                connection_status = "Connected"

                async for message in ws:
                    data = json.loads(message)

                    if not isinstance(data, list) or len(data) < 4:
                        continue

                    if data[2] != "ticker":
                        continue

                    ticker = data[1]
                    pair = data[3]

                    prices[pair] = {
                        "last": float(ticker["c"][0]),
                        "bid": float(ticker["b"][0]),
                        "ask": float(ticker["a"][0]),
                        "volume_24h": float(ticker["v"][1]),
                        "high_24h": float(ticker["h"][1]),
                        "low_24h": float(ticker["l"][1]),
                        "updated_at": datetime.now(
                            timezone.utc
                        ).isoformat()
                    }

        except Exception as error:
            connection_status = "Reconnecting"
            print("Kraken error:", error)
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    asyncio.create_task(kraken_feed())


@app.get("/")
async def home():
    return {
        "name": "Nova XRP Live Bridge",
        "status": connection_status,
        "mode": "READ ONLY",
        "trading_enabled": False
    }


@app.get("/health")
async def health():
    return {
        "status": connection_status,
        "prices_received": len(prices)
    }


@app.get("/latest")
async def latest():
    return {
        "bridge": "Nova XRP Live Bridge",
        "connection": connection_status,
        "prices": prices
    }
