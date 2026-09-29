"""Lake Writer (V4)

Consumes all 4 Kafka topics, buffers events, and writes them as Parquet
files into data-lake/landing/<topic>/<date>/.

Improvements over the first version:
  • Offsets are committed to Kafka ONLY after the Parquet file is safely on
    disk, so a crash never loses events (at-least-once delivery).
  • Files are written as *.tmp and renamed when complete, so the uploader
    never picks up a half-written file.
  • Each row gets a deterministic event_id (topic-partition-offset) and the
    real Kafka event time, which makes deduplication in Databricks easy.
"""
from confluent_kafka import Consumer, KafkaException
from collections import defaultdict
from datetime import datetime, timezone
import json
import os
import signal
import sys
import time
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from lake_config import (KAFKA_BOOTSTRAP, TOPICS, LANDING_DIR,
                         FLUSH_INTERVAL_SECONDS, FLUSH_BATCH_SIZE)

# ── Kafka Consumer ────────────────────────────────────────────────
consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'lake-writer-v4',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,      # we commit manually after each flush
})
consumer.subscribe(TOPICS)

print("🏔️  Lake Writer V4 is running!")
print(f"👂 Topics: {', '.join(TOPICS)}")
print(f"📁 Landing folder: {LANDING_DIR}")
print(f"⏱️  Flush every {FLUSH_INTERVAL_SECONDS}s or {FLUSH_BATCH_SIZE} events")
print("   Press Ctrl+C to stop\n")

buffers = defaultdict(list)
event_counter = 0
running = True


def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Lake Writer...")
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def write_parquet(topic, rows):
    """Write one topic's rows to landing/<topic>/<date>/batch_*.parquet."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    output_dir = os.path.join(LANDING_DIR, topic, today)
    os.makedirs(output_dir, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime('%H%M%S')
    filename = f"batch_{stamp}_{uuid.uuid4().hex[:8]}.parquet"
    final_path = os.path.join(output_dir, filename)
    temp_path = final_path + '.tmp'

    df = pd.DataFrame(rows)
    df.to_parquet(temp_path, engine='fastparquet', index=False)
    os.replace(temp_path, final_path)   # atomic rename: file is now "ready"
    print(f"  💾 {len(rows)} events → {os.path.relpath(final_path, LANDING_DIR)}")


def flush_all():
    """Write every buffer to disk, then commit Kafka offsets."""
    global event_counter
    if event_counter == 0:
        return
    for topic, rows in buffers.items():
        if rows:
            write_parquet(topic, rows)
    buffers.clear()
    event_counter = 0

    # Only now is it safe to tell Kafka "we've got these events"
    try:
        consumer.commit(asynchronous=False)
        print("  ✅ Kafka offsets committed\n")
    except KafkaException as e:
        print(f"  ⚠️  Offset commit failed (events may be re-read later): {e}\n")


last_flush = time.time()

try:
    while running:
        message = consumer.poll(timeout=1.0)

        if message is not None:
            if message.error():
                print(f"❌ Error: {message.error()}")
            elif message.value() is not None:
                topic = message.topic()
                key = message.key().decode('utf-8') if message.key() else ''
                ts_type, ts_ms = message.timestamp()
                now = datetime.now(timezone.utc)

                buffers[topic].append({
                    'event_id': f"{topic}-{message.partition()}-{message.offset()}",
                    'event_topic': topic,
                    'event_key': key,
                    'event_partition': int(message.partition()),
                    'event_offset': int(message.offset()),
                    # When the producer created the event (epoch millis)
                    'kafka_timestamp_ms': int(ts_ms) if ts_type != 0 else int(now.timestamp() * 1000),
                    # When the lake writer received it
                    'ingested_at': now.isoformat(),
                    # Raw payload, untouched (Bronze = store everything as-is)
                    'event_data': message.value().decode('utf-8'),
                })
                event_counter += 1
                print(f"📥 Buffered #{event_counter}: {topic} key={key} offset={message.offset()}")

        # Flush on size OR time (checked every loop, not only when idle)
        if event_counter >= FLUSH_BATCH_SIZE:
            print(f"\n📦 Batch size reached ({FLUSH_BATCH_SIZE})")
            flush_all()
            last_flush = time.time()
        elif event_counter > 0 and time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
            print(f"\n⏱️  Flush interval reached ({FLUSH_INTERVAL_SECONDS}s)")
            flush_all()
            last_flush = time.time()

finally:
    if event_counter > 0:
        print(f"\n💾 Flushing {event_counter} remaining events before shutdown...")
        flush_all()
    consumer.close()
    print("✅ Lake Writer shut down cleanly.")