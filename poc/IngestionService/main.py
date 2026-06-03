import asyncio
import json
import logging
import os
import uuid
import websockets
from collections import defaultdict
from datetime import datetime, timezone
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}'
)
log = logging.getLogger(__name__)

ALPACA_WS_URL = os.environ["ALPACA_WS_URL"]
TOPIC_RAW = "market.prices.raw"
TOPICS_BY_EVENT_TYPE: dict[str, str] = {
    "TRADE": "market.trades",
    "QUOTE": "market.prices",
    "AGGREGATE": "market.prices"
}
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
sequence_counters: defaultdict[str, int] = defaultdict(int)

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )

def normalize_event(raw: dict, counters: dict) -> dict | None:
    """
    Map an Alpaca message to the Muninn canonical envelope.
    Returns None for control messages (subscriptions, auth confirmations).
    """
    msg_type = raw.get("T")

    if msg_type == "t":
        event_type = "TRADE"
    elif msg_type == "q":
        event_type = "QUOTE"
    elif msg_type == "b":
        event_type = "AGGREGATE"
    else:
        return None

    symbol = raw.get("S", "UNKNOWN")
    counters[symbol] += 1

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "symbol": symbol,
        "timestamp_utc": raw.get("t"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "alpaca",
        "payload": raw,
        "sequence_num": counters[symbol]
    }

def publish_event(producer: KafkaProducer, raw_event: dict, envelope: dict) -> None:
    """
    Dual-publish: raw event to the immutable replay topic, normalized envelop to the appropriate downstream topic
    based on event type
    """
    symbol = envelope["symbol"]
    event_type = envelope["event_type"]

    producer.send(TOPIC_RAW, key=symbol, value=raw_event)

    normalized_topic = TOPICS_BY_EVENT_TYPE.get(event_type)
    if normalized_topic:
        producer.send(normalized_topic, key=symbol, value=envelope)
        log.info(
            "Published [%s] %s seq=%s -> %s",
            event_type,
            symbol,
            envelope["sequence_num"],
            normalized_topic
        )
    else:
        log.warning("No downstream topic configured for event_type=%s - raw only", event_type)

async def connect_and_ingest(producer: KafkaProducer) -> None:
    auth_msg = {
        "action": "auth",
        "key": os.environ["ALPACA_API_KEY"],
        "secret": os.environ["ALPACA_SECRET_KEY"]
    }
    subscribe_msg = {
        "action": "subscribe",
        "trades": ["FAKEPACA"],
        "quotes": ["FAKEPACA"],
        "bars": ["FAKEPACA"],
    }

    async with websockets.connect(ALPACA_WS_URL) as ws:
        log.info("WebSocket Connected")
        log.debug("Connected response: %s", await ws.recv())

        await ws.send(json.dumps(auth_msg))
        log.info("Auth sent")
        log.debug("Auth response: %s", await ws.recv())

        await ws.send(json.dumps(subscribe_msg))
        log.info("Subscribe sent")
        log.debug("Subscribe response: %s", await ws.recv())

        async for raw_message in ws:
            events = json.loads(raw_message)

            # Alpaca sends arrays of events
            for raw_event in events:
                envelope = normalize_event(raw_event, sequence_counters)
                if envelope is None:
                    continue

                publish_event(producer, raw_event, envelope)

async def run_with_reconnect(producer: KafkaProducer) -> None:
    delay = RECONNECT_BASE_DELAY
    while True:
        try:
            await connect_and_ingest(producer)
        except (websockets.ConnectionClosed, OSError) as e:
            log.warning("Connection lost: %s. Reconnecting in %ss...", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
        except Exception as e:
            log.error("Unexpected error: %s. Reconnecting in %ss...", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
        else:
            # reset the backoff on exit
            delay = RECONNECT_BASE_DELAY

if __name__ == '__main__':
    producer = create_producer()
    try:
        asyncio.run(run_with_reconnect(producer))
    finally:
        producer.flush()
        producer.close()
        log.info("Produce closed")