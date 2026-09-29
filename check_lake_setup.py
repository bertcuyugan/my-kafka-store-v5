"""Quick connectivity test for every destination in LAKE_SINKS.

Uploads a tiny text file to <destination>/_healthcheck/ping.txt.
(_healthcheck is outside raw/, so Auto Loader never reads it.)
"""
import os
import tempfile
from datetime import datetime, timezone

from lake_config import LAKE_SINKS
from sinks import build_sinks

print(f"Testing destinations: {', '.join(LAKE_SINKS)}\n")

with tempfile.NamedTemporaryFile('w', suffix='.txt', delete=False) as tmp:
    tmp.write(f"ping from kafka store at {datetime.now(timezone.utc).isoformat()}\n")
    tmp_path = tmp.name

all_ok = True
try:
    for sink in build_sinks(LAKE_SINKS):
        try:
            sink.check()
            remote = sink.upload(tmp_path, '_healthcheck/ping.txt')
            print(f"✅ {sink.name}: uploaded {remote}")
        except Exception as e:
            all_ok = False
            print(f"❌ {sink.name}: {type(e).__name__}: {e}")
finally:
    os.remove(tmp_path)

print("\nAll good — you can start the lake services." if all_ok
      else "\nFix the errors above before starting lake_uploader.py.")