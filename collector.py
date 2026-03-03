import os
import time
import json
import pyodbc
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────
load_dotenv()

DB_PATH = os.getenv("DB_PATH")
BACKEND_HTTP = os.getenv("BACKEND_HTTP")  # existing (events)
LIVE_STATUS_HTTP = os.getenv("LIVE_STATUS_HTTP")  # live statuses

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", 40))

# Pakistan timezone offset (naive local time)
PKT_OFFSET = timedelta(hours=5)

# Get user's home directory for persistent files
user_home = Path.home()
documents = user_home / "Documents"
QUEUE_FILE = documents / "washing_machine_event_queue.json"
STATE_FILE = documents / "washing_machine_collector_state.json"

# ─────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────
last_timestamp = None
last_statuses = {}
last_sent_statuses = {}          # NEW: track what was last sent to live-status endpoint
open_events = {}
event_queue = []

# ─────────────────────────────────────────────
# STATE LOAD / SAVE
# ─────────────────────────────────────────────
def load_state():
    global last_timestamp, last_statuses, last_sent_statuses, open_events, event_queue

    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            last_timestamp = (
                datetime.fromisoformat(state["last_timestamp"])
                if state.get("last_timestamp")
                else None
            )
            last_statuses = state.get("last_statuses", {})
            last_sent_statuses = state.get("last_sent_statuses", {})  # NEW
            open_events = state.get("open_events", {})
        except Exception as e:
            print(f"⚠️ Error loading state: {e}")

    if QUEUE_FILE.exists():
        try:
            event_queue[:] = json.loads(QUEUE_FILE.read_text())  # safer replace
        except Exception as e:
            print(f"⚠️ Error loading queue: {e}")
            event_queue.clear()

def save_state():
    try:
        STATE_FILE.write_text(
            json.dumps(
                {
                    "last_timestamp": last_timestamp.isoformat() if last_timestamp else None,
                    "last_statuses": last_statuses,
                    "last_sent_statuses": last_sent_statuses,          # NEW
                    "open_events": open_events,
                },
                indent=2,
            )
        )
        QUEUE_FILE.write_text(json.dumps(event_queue, indent=2))
    except Exception as e:
        print(f"⚠️ Error saving state: {e}")

# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────
def get_connection():
    return pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DB_PATH + ";",
        timeout=5,
    )

def parse_timestamp(ts):
    if isinstance(ts, datetime):
        return ts

    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.strptime(ts, fmt)
        except Exception:
            continue

    return None

def detect_status(power, downtime):
    if not power:
        return "OFF"
    if downtime:
        return "DOWNTIME"
    return "RUNNING"

# ─────────────────────────────────────────────
# FETCH RECORDS
# ─────────────────────────────────────────────
def fetch_new_records(cursor, last_ts):
    try:
        if last_ts:
            cursor.execute(
                "SELECT * FROM TREND001 WHERE Time_Stamp > ? ORDER BY Time_Stamp ASC",
                (last_ts,),
            )
        else:
            cursor.execute("SELECT * FROM TREND001 ORDER BY Time_Stamp ASC")

        rows = cursor.fetchall()
        if not rows:
            return []

        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, r)) for r in rows]

    except Exception as e:
        print("⚠️ DB fetch error:", e)
        return []

# ─────────────────────────────────────────────
# EVENT QUEUE
# ─────────────────────────────────────────────
def queue_event(event):
    event_queue.append(event)
    save_state()

def flush_queue():
    if not event_queue:
        return

    try:
        r = requests.post(BACKEND_HTTP, json=event_queue, timeout=10)
        if r.status_code in (200, 201):
            event_queue.clear()
            save_state()
    except Exception:
        pass  # retry next loop

# ─────────────────────────────────────────────
# LIVE STATUS UPDATER - only send when changed
# ─────────────────────────────────────────────
def update_live_statuses(record, ts):
    """
    Only send live status update when at least one machine's status changed.
    """
    if not LIVE_STATUS_HTTP:
        return

    current_statuses = {}

    for key, val in record.items():
        if not key.lower().endswith("power"):
            continue

        machine = key.replace("_Power", "").replace("Power", "")
        power = bool(val)

        downtime = False
        for dk in (
            f"{machine}_DownTime",
            f"{machine}_Downtime",
            f"{machine}DownTime",
            f"{machine}Downtime",
        ):
            if dk in record:
                downtime = bool(record[dk])
                break

        status = detect_status(power, downtime)
        current_statuses[machine] = status

    # Skip if nothing changed since last send
    if current_statuses == last_sent_statuses:
        return

    # Build payload
    payload = [
        {
            "machine": machine,
            "status": status,
            "updatedAt": ts.isoformat(),
        }
        for machine, status in current_statuses.items()
    ]

    if not payload:
        return

    try:
        requests.put(LIVE_STATUS_HTTP, json=payload, timeout=5)
        print(f"📡 Live statuses updated: {len(payload)} machines (changes detected)")
        last_sent_statuses.clear()
        last_sent_statuses.update(current_statuses)  # remember what we just sent
    except Exception as e:
        print(f"⚠️ Live status update failed: {e}")

# ─────────────────────────────────────────────
# PROCESS RECORDS
# ─────────────────────────────────────────────
def process_records(records):
    global last_timestamp

    for rec in records:
        ts = parse_timestamp(rec.get("Time_Stamp"))
        if not ts:
            continue

        if last_timestamp and ts <= last_timestamp:
            continue

        for key, val in rec.items():
            if not key.lower().endswith("power"):
                continue

            machine = key.replace("_Power", "").replace("Power", "")
            power = bool(val)

            downtime = False
            for dk in (
                f"{machine}_DownTime",
                f"{machine}_Downtime",
                f"{machine}DownTime",
                f"{machine}Downtime",
            ):
                if dk in rec:
                    downtime = bool(rec[dk])
                    break

            status = detect_status(power, downtime)
            prev_status = last_statuses.get(machine)

            if status == prev_status:
                continue

            if machine in open_events:
                prev = open_events[machine]
                prev_ts = datetime.fromisoformat(prev["timestamp"])
                prev["durationSeconds"] = round((ts - prev_ts).total_seconds(), 2)
                queue_event(prev)

            open_events[machine] = {
                "timestamp": ts.isoformat(),
                "machine": machine,
                "status": status,
                "machinePower": power,
                "downtime": downtime,
                "durationSeconds": 0,
            }

            last_statuses[machine] = status

        last_timestamp = ts
        save_state()

# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
def main():
    print("🚀 Collector running (events + live statuses)")
    load_state()

    while True:
        try:
            conn = get_connection()
            cursor = conn.cursor()

            records = fetch_new_records(cursor, last_timestamp)

            if records:
                process_records(records)

                # Use the LATEST row for live status snapshot
                latest = records[-1]
                ts = parse_timestamp(latest.get("Time_Stamp"))
                if ts:
                    update_live_statuses(latest, ts)

            flush_queue()

            cursor.close()
            conn.close()

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            print(f"🔥 Collector error: {e}")
            time.sleep(5)

# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()