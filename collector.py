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

DB_PATH          = os.getenv("DB_PATH")
SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY     = os.getenv("SUPABASE_SERVICE_KEY")
POLL_INTERVAL    = float(os.getenv("POLL_INTERVAL", 60))

PKT_OFFSET       = timedelta(hours=5)

# Performance / observability
FETCH_BATCH_SIZE         = int(os.getenv("FETCH_BATCH_SIZE", "5000"))
SUPABASE_BATCH_SIZE      = int(os.getenv("SUPABASE_BATCH_SIZE", "1000"))
STATE_SAVE_EVERY_RECORDS = int(os.getenv("STATE_SAVE_EVERY_RECORDS", "2000"))
STATE_SAVE_EVERY_SECONDS = float(os.getenv("STATE_SAVE_EVERY_SECONDS", "10"))
LOG_EVERY_RECORDS        = int(os.getenv("LOG_EVERY_RECORDS", "5000"))

# Supabase retry settings
SUPABASE_MAX_RETRIES = int(os.getenv("SUPABASE_MAX_RETRIES", "5"))
SUPABASE_RETRY_BASE  = float(os.getenv("SUPABASE_RETRY_BASE", "2.0"))   # exponential base (seconds)
SUPABASE_RETRY_MAX   = float(os.getenv("SUPABASE_RETRY_MAX", "60.0"))   # max wait between retries

# Access DB connection retry
DB_MAX_RETRIES   = int(os.getenv("DB_MAX_RETRIES", "3"))
DB_RETRY_DELAY   = float(os.getenv("DB_RETRY_DELAY", "3.0"))

# Persistent state files (crash recovery)
user_home  = Path.home()
documents  = user_home / "Documents"
QUEUE_FILE = documents / "washing_machine_event_queue.json"
STATE_FILE = documents / "washing_machine_collector_state.json"

# Temp files for atomic writes
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
# STATE LOAD / SAVE  (atomic writes)
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
    """
    Atomic save: write to .tmp then rename so a crash mid-write never corrupts the file.
    """
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
    """
    Connect to Access DB with retry logic.
    Retries up to DB_MAX_RETRIES times if the file is locked or temporarily unavailable.
    """
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
    Parse a timestamp from the Access DB.
    The DB stores PKT local time with no timezone info (naive datetime).
    e.g. '2026-03-03 16:00:00'  means  4:00 PM PKT.
    We return the value as-is (still naive/PKT) — callers handle conversion.
    """
    if isinstance(ts, datetime):
        return ts
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
    """
    Determine the shift from a PKT-naive datetime (as read from Access DB).

    The Access DB stores local PKT time directly, so ts.hour IS already the
    PKT hour — no offset addition needed.

    Shifts:
      Morning : 07:00 – 14:59 PKT
      Evening : 15:00 – 22:59 PKT
      Night   : 23:00 – 06:59 PKT
    """
    pkt_hour = ts.hour
    if 7 <= pkt_hour < 15:
        return "Morning"
    if 15 <= pkt_hour < 23:
        return "Evening"
    return "Night"


def to_utc_iso(ts: datetime) -> str:
    """
    Convert a PKT-naive datetime (from Access DB) to a UTC ISO-8601 string.
    PKT = UTC+5, so we subtract 5 hours.
    e.g.  datetime(2026,3,3,16,0,0)  →  '2026-03-03T11:00:00Z'
    """
    utc_dt = ts - PKT_OFFSET
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────
def supabase_insert_with_retry(table: str, rows: list) -> bool:
    """
    Insert rows into a Supabase table with exponential backoff retry.
    Returns True on success, False if all retries exhausted.
    """
    for attempt in range(1, SUPABASE_MAX_RETRIES + 1):
        try:
            supabase.table(table).insert(
                rows, returning="minimal", count=None
            ).execute()
            return True
        except Exception as e:
            wait = min(SUPABASE_RETRY_BASE ** attempt, SUPABASE_RETRY_MAX)
            if attempt < SUPABASE_MAX_RETRIES:
                print(f"⚠️  Supabase insert ({table}) attempt {attempt}/{SUPABASE_MAX_RETRIES} failed: {e} — retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"❌ Supabase insert ({table}) failed after {SUPABASE_MAX_RETRIES} attempts: {e}")
    return False


def supabase_upsert_with_retry(table: str, rows: list, on_conflict: str) -> bool:
    """
    Upsert rows into a Supabase table with exponential backoff retry.
    Returns True on success, False if all retries exhausted.
    """
    for attempt in range(1, SUPABASE_MAX_RETRIES + 1):
        try:
            supabase.table(table).upsert(rows, on_conflict=on_conflict).execute()
            return True
        except Exception as e:
            wait = min(SUPABASE_RETRY_BASE ** attempt, SUPABASE_RETRY_MAX)
            if attempt < SUPABASE_MAX_RETRIES:
                print(f"⚠️  Supabase upsert ({table}) attempt {attempt}/{SUPABASE_MAX_RETRIES} failed: {e} — retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"❌ Supabase upsert ({table}) failed after {SUPABASE_MAX_RETRIES} attempts: {e}")
    return False


def check_supabase_connectivity():
    """
    Test Supabase connection on startup before doing any work.
    Raises an exception if unreachable so we fail fast.
    """
    try:
        supabase.table("live_status").select("machine_name").limit(1).execute()
        print("✅ Supabase connectivity check passed")
    except Exception as e:
        raise ConnectionError(f"❌ Supabase unreachable at startup: {e}")


def send_heartbeat():
    """
    Upsert a heartbeat row to collector_health table so the dashboard
    can show whether the collector is alive or dead.
    Schema: collector_health(id text PK, last_seen timestamptz, status text)
    """
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
    """
    Streaming fetch from Access using fetchmany().
    Yields lists of dict rows to avoid loading the whole table at once.
    """
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
    Push queued events to Supabase machine_events table.
    Uses retry logic — queue is only cleared after confirmed insert.
    timestamp stored as UTC ISO; shift derived from PKT-naive ts.hour.
    """
    if not event_queue:
        return

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
            del event_queue[:batch_size]
            _mark_dirty()
            maybe_save_state(force=True)
            continue

        print(f"⬆️  Inserting {len(rows_to_insert)} machine_events "
              f"(queue remaining: {max(len(event_queue) - len(batch), 0)})")

        success = supabase_insert_with_retry("machine_events", rows_to_insert)
        if success:
            del event_queue[:batch_size]
            flushed_total += len(rows_to_insert)
            _mark_dirty()
            maybe_save_state(force=True)
        else:
            print(f"⚠️  Flush paused — {len(event_queue)} events remain in queue (saved to disk)")
            maybe_save_state(force=True)
            break

    if flushed_total:
        print(f"✅ Flushed {flushed_total} events to Supabase")


# ─────────────────────────────────────────────
# LIVE STATUS
# ─────────────────────────────────────────────
def update_live_statuses(record: dict, ts: datetime):
    """
    Upsert current machine states into live_status table.
    updated_at is stored as UTC ISO. Also includes current shift.
    Supabase Realtime pushes changes to all subscribed React clients.
    """
    current_statuses = {}

    for key, val in record.items():
        if not key.lower().endswith("power"):
            continue

        machine = key.replace("_Power", "").replace("Power", "")
        power   = bool(val)

        downtime = False
        for dk in (
            f"{machine}_DownTime", f"{machine}_Downtime",
            f"{machine}DownTime",  f"{machine}Downtime",
        ):
            if dk in record:
                downtime = bool(record[dk])
                break

        current_statuses[machine] = detect_status(power, downtime)

    # Skip if nothing changed
    if current_statuses == last_sent_statuses:
        return

    rows_to_upsert = [
        {
            "machine_name": machine,
            "status":       status,
            "shift":        detect_shift(ts),   # ← shift now included
            "updated_at":   to_utc_iso(ts),
        }
        for machine, status in current_statuses.items()
    ]

    if not rows_to_upsert:
        return

    total = 0
    all_ok = True
    for i in range(0, len(rows_to_upsert), SUPABASE_BATCH_SIZE):
        chunk = rows_to_upsert[i : i + SUPABASE_BATCH_SIZE]
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
    global last_timestamp
    global _records_since_save

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

            machine = key.replace("_Power", "").replace("Power", "")
            power   = bool(val)

            downtime = False
            for dk in (
                f"{machine}_DownTime", f"{machine}_Downtime",
                f"{machine}DownTime",  f"{machine}Downtime",
            ):
                if dk in rec:
                    downtime = bool(rec[dk])
                    break

            status      = detect_status(power, downtime)
            prev_status = last_statuses.get(machine)

            if status == prev_status:
                continue

            status_changes += 1

            # Close the previous open event — compute its duration
            if machine in open_events:
                prev    = open_events[machine]
                prev_ts = datetime.fromisoformat(prev["timestamp"])
                prev["durationSeconds"] = round((ts - prev_ts).total_seconds(), 2)
                queue_event(prev)
                events_queued += 1

            # Open a new event.
            # Store timestamp as PKT-naive isoformat so detect_shift works correctly
            # in flush_queue (ts.hour = PKT hour, no offset math needed).
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

        last_timestamp = ts
        processed      += 1
        _records_since_save += 1

        if processed % LOG_EVERY_RECORDS == 0:
            print(
                f"⏳ Processed {total_processed_so_far + processed:,} records "
                f"(batch {processed:,}); status changes: {status_changes:,}; "
                f"events queued: {events_queued:,}; queue size: {len(event_queue):,}"
            )

        maybe_save_state()

    maybe_save_state(force=True)
    return processed


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
def main():
    print("🚀 Collector running → Supabase (events + live statuses)")
    print(f"   Supabase URL : {SUPABASE_URL}")
    print(f"   Poll interval: {POLL_INTERVAL}s")
    print(f"   DB path      : {DB_PATH}")
    print()
    print("   Timezone contract:")
    print("   • Access DB  → PKT naive (no tz info)")
    print("   • detect_shift uses ts.hour directly (already PKT)")
    print("   • to_utc_iso subtracts 5h → real UTC stored in Supabase")
    print("   • Frontend adds 5h back for display → shows correct PKT")
    print()

    # ── Startup checks ──────────────────────────────────────────────────────
    if DB_PATH and not Path(DB_PATH).exists():
        print(f"❌ DB_PATH does not exist: {DB_PATH}")
        print("   Fix your .env DB_PATH and restart.")
        return

    try:
        check_supabase_connectivity()
    except ConnectionError as e:
        print(e)
        print("   Fix your SUPABASE_URL / SUPABASE_SERVICE_KEY and restart.")
        return

    load_state()

    # Flush anything left in queue from a previous crashed run
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

                # Flush periodically during large backfills
                if len(event_queue) >= SUPABASE_BATCH_SIZE:
                    flush_queue()

            if not any_batches:
                print("ℹ️  No new rows found in TREND001.")

            # Push live status snapshot from the latest row we saw
            if last_batch_latest is not None:
                ts = parse_timestamp(last_batch_latest.get("Time_Stamp"))
                if ts:
                    update_live_statuses(last_batch_latest, ts)

            flush_queue()

            cursor.close()
            conn.close()
            maybe_save_state(force=True)

            # ── Heartbeat ────────────────────────────────────────────────────
            send_heartbeat()

            # ── Per-cycle summary ────────────────────────────────────────────
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

    # ── Clean shutdown ───────────────────────────────────────────────────────
    print("💾 Saving state before exit...")
    maybe_save_state(force=True)

    if event_queue:
        print(f"⬆️  Flushing {len(event_queue)} remaining events before exit...")
        flush_queue()

    # Mark collector as offline in Supabase
    try:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        supabase.table("collector_health").upsert(
            {"id": "main", "last_seen": now_utc, "status": "offline"},
            on_conflict="id",
        ).execute()
    except Exception:
        pass

    print("👋 Collector shut down cleanly.")


# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()