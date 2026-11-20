
import json
import os
import random
import signal
import ssl
import sys
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

from match_data_config import (
    DEVICE_TYPES,
    DEVICE_WEIGHTS,
    EMOJI_CATALOG,
    HUB_TRAFFIC_WEIGHTS,
    INDIAN_FAN_HUBS,
    QUALITY_WEIGHTS,
    STREAM_QUALITIES,
)

# Load credentials from .env
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_EMOJI", "emoji_reaction_stream")
MATCH_ID = os.getenv("MATCH_ID", "IPL_2026_FINAL_CSK_MI")
TARGET_EPS = int(os.getenv("EVENTS_PER_SECOND", "50"))

if not all([BOOTSTRAP_SERVERS, API_KEY, API_SECRET]):
    print("❌ ERROR: Missing Confluent credentials in .env file.")
    sys.exit(1)

# Confluent Cloud SASL_SSL Producer Configuration
ssl_context = ssl.create_default_context()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=API_KEY,
    sasl_plain_password=API_SECRET,
    ssl_context=ssl_context,
    client_id="ipl-fan-pulse-laptop-producer",
    acks=1,
    linger_ms=20,
    batch_size=32768,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Pre-build a pool of 25,000 realistic active viewers


def build_viewer_pool(pool_size: int = 25000) -> list[dict]:
    pool = []
    for i in range(1, pool_size + 1):
        hub = random.choices(
            INDIAN_FAN_HUBS, weights=HUB_TRAFFIC_WEIGHTS, k=1)[0]
        teams = list(hub["team_weights"].keys())
        t_weights = list(hub["team_weights"].values())
        supported_team = random.choices(teams, weights=t_weights, k=1)

        pool.append({
            "user_id": f"USR_{i:06d}",
            "session_id": f"SES_{uuid.uuid4().hex[:10].upper()}",
            "supported_team": supported_team,
            "user_city": hub["city"],
            "user_state": hub["state"],
            "device_type": random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1),
            "stream_quality": random.choices(STREAM_QUALITIES, weights=QUALITY_WEIGHTS, k=1),
            "last_tap_ts": 0.0,
            "streak": 0,
        })
    return pool


EMOJI_IDS = [e["emoji_id"] for e in EMOJI_CATALOG]
EMOJI_WEIGHTS = [e["weight"] for e in EMOJI_CATALOG]


def generate_emoji_event(viewer_pool: list[dict]) -> dict:
    viewer = random.choice(viewer_pool)
    now = time.time()

    if now - viewer["last_tap_ts"] <= 3.0:
        viewer["streak"] += 1
    else:
        viewer["streak"] = 1
    viewer["last_tap_ts"] = now

    emoji_id = random.choices(EMOJI_IDS, weights=EMOJI_WEIGHTS, k=1)[0]

    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "match_id": MATCH_ID,
        "user_id": viewer["user_id"],
        "session_id": viewer["session_id"],
        "supported_team": viewer["supported_team"][0],
        "user_city": viewer["user_city"],
        "user_state": viewer["user_state"],
        "device_type": viewer["device_type"][0],
        "stream_quality": viewer["stream_quality"][0],
        "emoji_id": emoji_id,
        "tap_streak": viewer["streak"],
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


delivery_errors = 0


def on_send_error(excp):
    global delivery_errors
    delivery_errors += 1
    if delivery_errors <= 5:
        print(f"⚠️ Delivery failed: {excp}")


running = True


def shutdown_handler(sig, frame):
    global running
    print("\n🛑 Stopping producer gracefully and flushing remaining messages...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def main():
    print("Initializing 25,000 simulated IPL viewers across India...")
    viewer_pool = build_viewer_pool(25000)
    print(
        f"🚀 Streaming to Confluent topic '{TOPIC_NAME}' at {TARGET_EPS} events/sec (Press Ctrl+C to stop)\n")

    total_sent = 0
    start_time = time.time()

    while running:
        batch_start = time.time()

        for _ in range(TARGET_EPS):
            event = generate_emoji_event(viewer_pool)
            producer.send(
                topic=TOPIC_NAME,
                key=event["user_id"],
                value=event,
            ).add_errback(on_send_error)
            total_sent += 1

        elapsed_sec = int(time.time() - start_time)
        if elapsed_sec > 0 and total_sent % (TARGET_EPS * 5) == 0:
            actual_rate = round(total_sent / (time.time() - start_time), 1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {total_sent:,} events | Avg Rate: {actual_rate} msg/sec | Errors: {delivery_errors}")

        batch_duration = time.time() - batch_start
        sleep_time = max(0.0, 1.0 - batch_duration)
        if sleep_time > 0 and running:
            time.sleep(sleep_time)

    producer.flush(timeout=10)
    producer.close()
    print(f"✅ Clean shutdown complete. Total events delivered: {total_sent:,}")


if __name__ == "__main__":
    main()
