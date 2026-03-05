import os
import signal
import time
import json
import pyodbc
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from supabase import create_client, Client

# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────
load_dotenv()

DB_PATH       = os.getenv("DB_PATH")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_KEY")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", 60))

PKT_OFFSET = timedelta(hours=5)

# Performance / observability
FETCH_BATCH_SIZE         = int(os.getenv("FETCH_BATCH_SIZE", "5000"))
SUPABASE_BATCH_SIZE      = int(os.getenv("SUPABASE_BATCH_SIZE", "500"))
STATE_SAVE_EVERY_RECORDS = int(os.getenv("STATE_SAVE_EVERY_RECORDS", "2000"))
STATE_SAVE_EVERY_SECONDS = float(os.getenv("STATE_SAVE_EVERY_SECONDS", "10"))
LOG_EVERY_RECORDS        = int(os.getenv("LOG_EVERY_RECORDS", "5000"))

# Supabase retry settings
SUPABASE_MAX_RETRIES = int(os.getenv("SUPABASE_MAX_RETRIES", "5"))
SUPABASE_RETRY_BASE  = float(os.getenv("SUPABASE_RETRY_BASE", "2.0"))
SUPABASE_RETRY_MAX   = float(os.getenv("SUPABASE_RETRY_MAX", "60.0"))

# Access DB connection retry
DB_MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", "3"))
DB_RETRY_DELAY = float(os.getenv("DB_RETRY_DELAY", "3.0"))

# Persistent state files (crash recovery)
user_home      = Path.home()
documents      = user_home / "Documents"
QUEUE_FILE     = documents / "washing_machine_event_queue.json"
STATE_FILE     = documents / "washing_machine_collector_state.json"
QUEUE_FILE_TMP = documents / "washing_machine_event_queue.json.tmp"
STATE_FILE_TMP = documents / "washing_machine_collector_state.json.tmp"

# ─────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# ─────────────────────────────────────────────
_shutdown_requested = False

def _handle_signal(signum, frame):
    global _shutdown_requested
    print("\n🛑 Shutdown signal received — finishing current work and saving state...")
    _shutdown_requested = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ─────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────
last_timestamp     = None
last_statuses      = {}
last_sent_statuses = {}
open_events        = {}
event_queue        = []

_last_save_monotonic = time.monotonic()
_records_since_save  = 0
_dirty               = False


def _mark_dirty():
    global _dirty
    _dirty = True


# ─────────────────────────────────────────────
# STATE LOAD / SAVE (atomic writes)
# ─────────────────────────────────────────────
def load_state():
    global last_timestamp, last_statuses, last_sent_statuses, open_events, event_queue

    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            last_timestamp = (
                datetime.fromisoformat(state["last_timestamp"])
                if state.get("last_timestamp") else None
            )
            last_statuses      = state.get("last_statuses", {})
            last_sent_statuses = state.get("last_sent_statuses", {})
            open_events        = state.get("open_events", {})
            print(f"📂 State loaded — last timestamp: {last_timestamp} PKT")
            if open_events:
                print(f"   ↳ {len(open_events)} open events recovered from previous run")
        except Exception as e:
            print(f"⚠️  Error loading state: {e} — starting fresh")

    if QUEUE_FILE.exists():
        try:
            event_queue[:] = json.loads(QUEUE_FILE.read_text())
            if event_queue:
                print(f"📂 Queue loaded — {len(event_queue)} pending events recovered")
        except Exception as e:
            print(f"⚠️  Error loading queue: {e} — starting with empty queue")
            event_queue.clear()


def save_state():
    """Atomic save: write to .tmp then rename so a crash mid-write never corrupts the file."""
    try:
        state_data = json.dumps({
            "last_timestamp":     last_timestamp.isoformat() if last_timestamp else None,
            "last_statuses":      last_statuses,
            "last_sent_statuses": last_sent_statuses,
            "open_events":        open_events,
        }, indent=2)
        STATE_FILE_TMP.write_text(state_data)
        STATE_FILE_TMP.replace(STATE_FILE)

        queue_data = json.dumps(event_queue, indent=2)
        QUEUE_FILE_TMP.write_text(queue_data)
        QUEUE_FILE_TMP.replace(QUEUE_FILE)

    except Exception as e:
        print(f"⚠️  Error saving state: {e}")


def maybe_save_state(force: bool = False):
    global _last_save_monotonic, _records_since_save, _dirty
    if not _dirty and not force:
        return

    now = time.monotonic()
    if (force
            or _records_since_save >= STATE_SAVE_EVERY_RECORDS
            or (now - _last_save_monotonic) >= STATE_SAVE_EVERY_SECONDS):
        save_state()
        _last_save_monotonic = now
        _records_since_save  = 0
        _dirty               = False


# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────
def get_connection():
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            return pyodbc.connect(
                r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DB_PATH + ";",
                timeout=5,
            )
        except Exception as e:
            last_err = e
            if attempt < DB_MAX_RETRIES:
                print(f"⚠️  DB connect attempt {attempt}/{DB_MAX_RETRIES} failed: {e} — retrying in {DB_RETRY_DELAY}s")
                time.sleep(DB_RETRY_DELAY)
    raise ConnectionError(f"DB connection failed after {DB_MAX_RETRIES} attempts: {last_err}")


def parse_timestamp(ts) -> datetime | None:
    """
    Parse a timestamp from any of these sources:
      - datetime object (pass-through)
      - ISO 8601 string with T separator (from queue/state JSON): "2026-03-05T11:03:46"
      - ISO 8601 with microseconds: "2026-03-05T11:03:46.123456"
      - Access DB space-separated: "2026-03-05 11:03:46"
      - Access DB 12-hour format:  "3/5/2026 11:03:46 AM"
    """
    if isinstance(ts, datetime):
        return ts

    # fromisoformat handles all ISO variants: with T, with microseconds,
    # with/without timezone — covers everything saved by .isoformat() in the queue
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        pass

    # Fallback for Access DB formats not covered by fromisoformat
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.strptime(ts, fmt)
        except Exception:
            continue

    return None


def detect_status(power: bool, downtime: bool) -> str:
    if not power:
        return "OFF"
    if downtime:
        return "DOWNTIME"
    return "RUNNING"


def detect_shift(ts: datetime) -> str:
    h = ts.hour
    if 7 <= h < 15:
        return "Morning"
    if 15 <= h < 23:
        return "Evening"
    return "Night"


def to_utc_iso(ts: datetime) -> str:
    utc_dt = ts - PKT_OFFSET
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────

def supabase_insert_events_with_retry(rows: list) -> bool:
    """
    Insert rows into machine_events via Postgres RPC using
    INSERT ... ON CONFLICT DO NOTHING — safe for bulk inserts
    with duplicates, compatible with supabase-py 2.28.0.
    """
    if not rows:
        return True

    for attempt in range(1, SUPABASE_MAX_RETRIES + 1):
        try:
            supabase.rpc(
                "insert_machine_events_ignore_duplicates",
                {"events": rows}
            ).execute()

            print(f"   ✔ Inserted batch of {len(rows)} rows via RPC")
            return True

        except Exception as e:
            err_str = str(e)
            print(f"🔍 RPC INSERT ERROR (attempt {attempt}/{SUPABASE_MAX_RETRIES}): {err_str}")

            wait = min(SUPABASE_RETRY_BASE ** attempt, SUPABASE_RETRY_MAX)
            if attempt < SUPABASE_MAX_RETRIES:
                print(f"⚠️  Retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                print(f"❌ RPC insert failed after {SUPABASE_MAX_RETRIES} attempts.")

    return False


def supabase_upsert_with_retry(table: str, rows: list, on_conflict: str) -> bool:
    """Generic upsert helper used for live_status and other tables."""
    for attempt in range(1, SUPABASE_MAX_RETRIES + 1):
        try:
            supabase.table(table).upsert(rows, on_conflict=on_conflict).execute()
            return True
        except Exception as e:
            wait = min(SUPABASE_RETRY_BASE ** attempt, SUPABASE_RETRY_MAX)
            if attempt < SUPABASE_MAX_RETRIES:
                print(f"⚠️  Supabase upsert ({table}) attempt {attempt}/{SUPABASE_MAX_RETRIES} "
                      f"failed: {e} — retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"❌ Supabase upsert ({table}) failed after {SUPABASE_MAX_RETRIES} attempts: {e}")
    return False


def check_supabase_connectivity():
    try:
        supabase.table("live_status").select("machine_name").limit(1).execute()
        print("✅ Supabase connectivity check passed")
    except Exception as e:
        raise ConnectionError(f"❌ Supabase unreachable at startup: {e}")


def test_insert_machine_events() -> bool:
    """Test both direct insert AND the RPC function at startup."""
    test_row = {
        "machine_name":     "__test__",
        "timestamp":        "2000-01-01T00:00:00Z",
        "status":           "OFF",
        "machine_power":    False,
        "downtime":         False,
        "shift":            "Night",
        "duration_seconds": 0.0,
    }

    # 1. Test direct insert
    try:
        response = supabase.table("machine_events").insert(
            test_row,
            returning="representation",
            count="exact",
        ).execute()

        inserted = response.count if response.count is not None else "unknown"
        print(f"   → Direct insert count: {inserted}")
        supabase.table("machine_events").delete().eq("machine_name", "__test__").execute()

        if response.count != 1:
            print(f"❌ Direct insert test failed: expected 1, got {inserted}")
            return False

        print("✅ Direct insert test PASSED")

    except Exception as e:
        print(f"❌ Direct insert test FAILED: {e}")
        return False

    # 2. Test RPC function
    try:
        response = supabase.rpc(
            "insert_machine_events_ignore_duplicates",
            {"events": [test_row]}
        ).execute()
        print(f"   → RPC response: {response}")
        supabase.table("machine_events").delete().eq("machine_name", "__test__").execute()
        print("✅ RPC insert test PASSED — function exists and is callable")
        return True

    except Exception as e:
        print(f"❌ RPC insert test FAILED: {e}")
        print("   → Run the CREATE FUNCTION SQL in Supabase SQL Editor first!")
        return False


def send_heartbeat():
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        supabase.table("collector_health").upsert(
            {"id": "main", "last_seen": now_utc, "status": "online"},
            on_conflict="id",
        ).execute()
    except Exception as e:
        print(f"⚠️  Heartbeat failed (non-critical): {e}")


# ─────────────────────────────────────────────
# FETCH RECORDS FROM ACCESS DB
# ─────────────────────────────────────────────
def fetch_new_records(cursor, last_ts, batch_size: int = FETCH_BATCH_SIZE):
    try:
        if last_ts:
            cursor.execute(
                "SELECT * FROM TREND001 WHERE Time_Stamp > ? ORDER BY Time_Stamp ASC",
                (last_ts,),
            )
        else:
            cursor.execute("SELECT * FROM TREND001 ORDER BY Time_Stamp ASC")

        cols = [c[0] for c in cursor.description]

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(zip(cols, r)) for r in rows]

    except Exception as e:
        print("⚠️  DB fetch error:", e)
        return


# ─────────────────────────────────────────────
# EVENT QUEUE
# ─────────────────────────────────────────────
def queue_event(event: dict):
    event_queue.append(event)
    _mark_dirty()


def flush_queue(batch_size: int = SUPABASE_BATCH_SIZE):
    """
    Push queued events to Supabase machine_events.
    Queue is only cleared after a confirmed successful insert.
    """
    if not event_queue:
        return

    print(f"🔄 flush_queue called — {len(event_queue)} events pending")
    flushed_total = 0

    while event_queue:
        if _shutdown_requested:
            print("🛑 Shutdown — stopping flush, saving remaining queue to disk")
            maybe_save_state(force=True)
            break

        batch = event_queue[:batch_size]

        rows_to_insert = []
        for ev in batch:
            ts = parse_timestamp(ev.get("timestamp"))
            if not ts:
                print(f"⚠️  Could not parse timestamp: {ev.get('timestamp')!r} — skipping event")
                continue
            rows_to_insert.append({
                "machine_name":     ev["machine"],
                "timestamp":        to_utc_iso(ts),
                "status":           ev["status"],
                "machine_power":    ev.get("machinePower", False),
                "downtime":         ev.get("downtime", False),
                "shift":            detect_shift(ts),
                "duration_seconds": round(ev.get("durationSeconds", 0), 2),
            })

        if not rows_to_insert:
            # Every event in this batch had an unparseable timestamp — log and discard
            print(f"⚠️  Entire batch of {len(batch)} events had unparseable timestamps — discarding.")
            print(f"   Sample event: {batch[0] if batch else 'empty'}")
            del event_queue[:batch_size]
            _mark_dirty()
            maybe_save_state(force=True)
            continue

        print(f"⬆️  Inserting {len(rows_to_insert)} rows "
              f"(queue remaining after: {max(len(event_queue) - len(batch), 0)})")

        success = supabase_insert_events_with_retry(rows_to_insert)
        if success:
            del event_queue[:batch_size]
            flushed_total += len(rows_to_insert)
            _mark_dirty()
            maybe_save_state(force=True)
        else:
            print(f"⚠️  Flush paused — {len(event_queue)} events remain (saved to disk)")
            maybe_save_state(force=True)
            break

    if flushed_total:
        print(f"✅ Flushed {flushed_total} events to Supabase machine_events")


# ─────────────────────────────────────────────
# LIVE STATUS
# ─────────────────────────────────────────────
def update_live_statuses(record: dict, ts: datetime):
    current_statuses = {}

    for key, val in record.items():
        if not key.lower().endswith("power"):
            continue
        machine  = key.replace("_Power", "").replace("Power", "")
        power    = bool(val)
        downtime = False
        for dk in (f"{machine}_DownTime", f"{machine}_Downtime",
                   f"{machine}DownTime",  f"{machine}Downtime"):
            if dk in record:
                downtime = bool(record[dk])
                break
        current_statuses[machine] = detect_status(power, downtime)

    if current_statuses == last_sent_statuses:
        return

    rows_to_upsert = [
        {
            "machine_name": m,
            "status":       s,
            "shift":        detect_shift(ts),
            "updated_at":   to_utc_iso(ts),
        }
        for m, s in current_statuses.items()
    ]
    if not rows_to_upsert:
        return

    total  = 0
    all_ok = True
    for i in range(0, len(rows_to_upsert), SUPABASE_BATCH_SIZE):
        chunk = rows_to_upsert[i: i + SUPABASE_BATCH_SIZE]
        ok = supabase_upsert_with_retry("live_status", chunk, on_conflict="machine_name")
        if ok:
            total += len(chunk)
        else:
            all_ok = False

    if total:
        print(f"📡 Live statuses upserted: {total} machines (shift: {detect_shift(ts)})")

    if all_ok:
        last_sent_statuses.clear()
        last_sent_statuses.update(current_statuses)
        _mark_dirty()
        maybe_save_state()


# ─────────────────────────────────────────────
# PROCESS RECORDS
# ─────────────────────────────────────────────
def process_records(records: list, total_processed_so_far: int = 0) -> int:
    global last_timestamp, _records_since_save

    processed      = 0
    status_changes = 0
    events_queued  = 0

    for rec in records:
        if _shutdown_requested:
            break

        ts = parse_timestamp(rec.get("Time_Stamp"))
        if not ts:
            continue
        if last_timestamp and ts <= last_timestamp:
            continue

        for key, val in rec.items():
            if not key.lower().endswith("power"):
                continue

            machine  = key.replace("_Power", "").replace("Power", "")
            power    = bool(val)
            downtime = False
            for dk in (f"{machine}_DownTime", f"{machine}_Downtime",
                       f"{machine}DownTime",  f"{machine}Downtime"):
                if dk in rec:
                    downtime = bool(rec[dk])
                    break

            status      = detect_status(power, downtime)
            prev_status = last_statuses.get(machine)
            if status == prev_status:
                continue

            status_changes += 1

            if machine in open_events:
                prev    = open_events[machine]
                prev_ts = datetime.fromisoformat(prev["timestamp"])
                prev["durationSeconds"] = round((ts - prev_ts).total_seconds(), 2)
                queue_event(prev)
                events_queued += 1

            open_events[machine] = {
                "timestamp":       ts.isoformat(),
                "machine":         machine,
                "status":          status,
                "machinePower":    power,
                "downtime":        downtime,
                "durationSeconds": 0,
            }
            last_statuses[machine] = status
            _mark_dirty()

        last_timestamp      = ts
        processed           += 1
        _records_since_save += 1

        if processed % LOG_EVERY_RECORDS == 0:
            print(
                f"⏳ Processed {total_processed_so_far + processed:,} records "
                f"| status changes: {status_changes:,} "
                f"| events queued: {events_queued:,} "
                f"| queue size: {len(event_queue):,}"
            )

        maybe_save_state()

    maybe_save_state(force=True)
    return processed


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
def main():
    print("🚀 Collector running → Supabase (events + live statuses)")
    print(f"   Supabase URL  : {SUPABASE_URL}")
    print(f"   Poll interval : {POLL_INTERVAL}s")
    print(f"   DB path       : {DB_PATH}")
    print(f"   Batch size    : {SUPABASE_BATCH_SIZE} rows/insert")
    print()

    if DB_PATH and not Path(DB_PATH).exists():
        print(f"❌ DB_PATH does not exist: {DB_PATH}")
        return

    try:
        check_supabase_connectivity()
    except ConnectionError as e:
        print(e)
        return

    print("🔬 Running startup insert test...")
    if not test_insert_machine_events():
        print("🛑 Aborting — fix the errors above, then restart.")
        return

    load_state()

    if event_queue:
        print(f"🔄 Flushing {len(event_queue)} recovered events from previous run...")
        flush_queue()

    cycle = 0

    while not _shutdown_requested:
        cycle += 1
        cycle_start = time.monotonic()

        try:
            conn   = get_connection()
            cursor = conn.cursor()

            total_processed   = 0
            last_batch_latest = None
            any_batches       = False

            for batch in fetch_new_records(cursor, last_timestamp):
                if _shutdown_requested:
                    break

                any_batches = True
                if not batch:
                    continue

                if last_batch_latest is None:
                    first_ts = batch[0].get("Time_Stamp")
                    print(f"📥 Fetched first batch: {len(batch):,} rows "
                          f"(starting from {first_ts} PKT)")

                last_batch_latest = batch[-1]
                processed = process_records(batch, total_processed_so_far=total_processed)
                total_processed += processed

                if len(event_queue) >= SUPABASE_BATCH_SIZE:
                    flush_queue()

            if not any_batches:
                print("ℹ️  No new rows found in TREND001.")

            if last_batch_latest is not None:
                ts = parse_timestamp(last_batch_latest.get("Time_Stamp"))
                if ts:
                    update_live_statuses(last_batch_latest, ts)

            flush_queue()

            cursor.close()
            conn.close()
            maybe_save_state(force=True)
            send_heartbeat()

            elapsed = time.monotonic() - cycle_start
            print(
                f"✔  Cycle {cycle} done in {elapsed:.1f}s — "
                f"processed: {total_processed:,} rows | "
                f"queue: {len(event_queue)} | "
                f"next poll in {POLL_INTERVAL:.0f}s"
            )

            if not _shutdown_requested:
                time.sleep(POLL_INTERVAL)

        except ConnectionError as e:
            print(f"🔥 DB connection error: {e}")
            maybe_save_state(force=True)
            if not _shutdown_requested:
                time.sleep(DB_RETRY_DELAY)

        except Exception as e:
            print(f"🔥 Collector error: {e}")
            maybe_save_state(force=True)
            if not _shutdown_requested:
                time.sleep(5)

    print("💾 Saving state before exit...")
    maybe_save_state(force=True)

    if event_queue:
        print(f"⬆️  Flushing {len(event_queue)} remaining events before exit...")
        flush_queue()

    try:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        supabase.table("collector_health").upsert(
            {"id": "main", "last_seen": now_utc, "status": "offline"},
            on_conflict="id",
        ).execute()
    except Exception:
        pass

    print("👋 Collector shut down cleanly.")


if __name__ == "__main__":
    main()