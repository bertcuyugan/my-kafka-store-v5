"""Lake Uploader (V4)

Watches data-lake/landing/ and pushes every finished Parquet file to the
cloud destinations listed in LAKE_SINKS (.env). After a file reaches ALL
destinations it is moved to data-lake/archive/.

If the internet drops or a token expires, files simply wait in landing/
and are retried on the next cycle — nothing is lost.
"""
import os
import signal
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lake_config import (LANDING_DIR, ARCHIVE_DIR, LAKE_SINKS,
                         UPLOAD_INTERVAL_SECONDS, REMOTE_RAW_PREFIX)
from sinks import build_sinks

running = True


def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping Lake Uploader...")
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def ready_files():
    """All finished Parquet files in landing/ (skips *.tmp files)."""
    found = []
    for root, _dirs, files in os.walk(LANDING_DIR):
        for name in files:
            if name.endswith('.parquet'):
                found.append(os.path.join(root, name))
    return sorted(found)


def archive(local_path, rel):
    archive_path = os.path.join(ARCHIVE_DIR, rel)
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    os.replace(local_path, archive_path)


def main():
    os.makedirs(LANDING_DIR, exist_ok=True)
    sinks = build_sinks(LAKE_SINKS)

    print("☁️  Lake Uploader V4 is running!")
    print(f"📁 Watching: {LANDING_DIR}")
    for sink in sinks:
        sink.check()   # fail fast if credentials/paths are wrong
        print(f"   ✅ Destination ready: {sink.name}")
    print(f"⏱️  Checking for new files every {UPLOAD_INTERVAL_SECONDS}s")
    print("   Press Ctrl+C to stop\n")

    while running:
        for local_path in ready_files():
            if not running:
                break
            rel = os.path.relpath(local_path, LANDING_DIR)
            remote_rel = f"{REMOTE_RAW_PREFIX}/{rel.replace(os.sep, '/')}"
            try:
                for sink in sinks:
                    remote = sink.upload(local_path, remote_rel)
                    print(f"  ☁️  [{sink.name}] {remote}")
                archive(local_path, rel)
            except Exception as e:
                # Leave the file in landing/ — it will be retried next cycle.
                # Re-uploading is safe: uploads overwrite the same path and
                # Auto Loader never processes the same file path twice.
                print(f"  ❌ Upload failed for {rel}: {e}")

        # Sleep in 1-second steps so Ctrl+C is responsive
        for _ in range(UPLOAD_INTERVAL_SECONDS):
            if not running:
                break
            time.sleep(1)

    print("✅ Lake Uploader shut down cleanly.")


if __name__ == '__main__':
    main()