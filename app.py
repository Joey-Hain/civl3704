"""
Web dashboard for TfNSW GTFS-realtime delay/variance data.

=== SCHEDULE LOADING ===

Operators and trip headsigns come from the TfNSW schedule bundle, downloaded
and parsed ON THE FIRST REQUEST that needs them, synchronously, under a lock.
The first request after a cold start takes ~30 seconds; every subsequent
request has full data immediately.

Why not a background thread: with gunicorn's worker model, a thread started
at module import time doesn't reliably share state with the process that
handles requests. Loading synchronously in the request handler eliminates
that whole class of problem.

RETRY-ON-FAILURE FIX: the cache is only considered "successfully loaded"
when agency_names is a non-empty dict (checked via truthiness, not `is not
None`). A prior version stored {} on failure and checked `is not None`,
which is true for {} too — so one transient failure permanently blanked
operator names and headsigns for the rest of the process's life. Failures
now retry after SCHEDULE_RETRY_BACKOFF_SEC instead of being cached forever.

Render's health check hits /ping, which never touches the schedule, so it
always returns "pong" in microseconds and never times out.

=== CONCURRENCY / MEMORY ===

get_all_rows_cached(), get_vehicles_cached(), and get_heatmap_points_cached()
are each guarded by a lock with double-checked locking. Without this, under
a threaded gunicorn worker (--threads N > 1), several requests can hit a
stale cache at the exact same instant — each one would then independently
re-fetch and re-parse the full feed (tens of thousands of trip-update rows,
~15MB+ per copy), multiplying peak memory by however many threads raced in
at once. This was the direct cause of an out-of-memory crash after
threading was enabled without these locks. The lock ensures only the first
thread to arrive does the actual fetch; everyone else waits briefly and
reuses its result.

Recommended Render start command (see render.yaml):
    gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 6 --worker-class gthread --timeout 120

--threads 32 (or even 8) is excessive for a 512MB instance even WITH the
locks restored — 6 is plenty of concurrency for page loads + the 15s
vehicle poll + occasional heatmap requests without inviting further memory
pressure from thread stacks and per-thread working memory.

=== HISTORICAL HEATMAP ===

/api/heatmap fetches daily CSVs from the gtfs-r-scrape repo synchronously
with a 20-second deadline, aggregating into rounded lat/lon grid cells
(GRID_DECIMALS) rather than keeping every raw point — this bounds output
size and memory regardless of how many readings a window covers. The
frontend auto-loads it on page view and every 5 minutes after.

/api/heatmap takes a &metric= of "delay" (default), "density", or "speed".
Fetching and aggregating a window's CSVs into grid cells is the expensive
part (network + parse), and is identical regardless of which metric the
caller wants — so that work is cached per window_hours only
(get_historical_cells_cached), independent of metric. Turning cached cells
into a metric's weighted points (compute_metric_points) is cheap pure
arithmetic done fresh on every request, so switching the metric dropdown
client-side never triggers a re-fetch.

Per-metric weighting:
  - delay: mean lateness (seconds late, floored at 0 so early running never
    cancels out a late reading elsewhere), not raw ping count — a busy
    interchange with mostly on-time buses should not outrank a quiet
    corridor where buses are consistently 15 minutes late. See
    HEATMAP_SEVERITY_CAP_SEC for the value that saturates the gradient.
  - speed: mean speed (km/h, from the collector's speed_kmh column),
    normalised against HEATMAP_SPEED_CAP_KMH. Hotter = faster, not
    slower — this is a "where do buses actually get to move" view, the
    mirror image of the delay heatmap rather than a congestion map.
  - density: raw ping count per cell, normalised against the busiest cell
    in the window (no confidence scaling — the count itself IS the
    sample size, unlike delay/speed which are means over however many
    readings landed in a cell). This is deliberately the simplest of the
    three: it exists mainly to prove the heatmap rendering pipeline is
    correct independent of any weighting logic.

delay and speed weight are both scaled by a confidence factor —
min(n_readings / HEATMAP_CONFIDENT_SAMPLES, 1) — so a cell backed by only
one or two readings fades toward the cool end even if that one reading was
extreme, rather than painting a full-strength hotspot off a single noisy
ping. A hard minimum-sample cutoff was tried first (for delay) and
rejected: the scrape cadence in practice is roughly every 2-3 hours (see
gtfs-r-scrape), so "Last hour" would almost always have exactly one
reading per cell and a cutoff of 2+ would leave that window essentially
blank.

=== RENDER SETTINGS ===

  Start Command (single line, no backslashes):
      gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 6 --worker-class gthread --timeout 120

  Health Check Path: /ping

  IMPORTANT: if you deploy via render.yaml (Blueprint), that file is the
  source of truth and can silently overwrite anything set manually in the
  dashboard on the next sync. Keep the Start Command in render.yaml, not
  just in the dashboard field, or your changes there may not stick.

Setup:
    pip install flask requests python-dotenv gtfs-realtime-bindings tzdata

.env file:
    TFNSW_API_KEY=your_api_key_here
    TFNSW_GTFS_RT_URL=https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses
"""

import codecs
import csv
import io
import json
import os
import re
import shutil
from html import escape as html_escape
import statistics
import tempfile
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request
from google.transit import gtfs_realtime_pb2

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
UTC_TZ = ZoneInfo("UTC")
DATA_DIR = Path("CIVL3704")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = DATA_DIR / "delay_log.csv"
ANOMALY_ABS_SEC = 3600
ON_TIME_EARLY_SEC = -60
ON_TIME_LATE_SEC = 300

GRID_DECIMALS = 4
HEATMAP_FETCH_CONCURRENCY = 1
HEATMAP_DEADLINE_SEC = 20
# Mean lateness (seconds) at which the heat gradient saturates. Chosen
# well above ON_TIME_LATE_SEC (300s) so the ramp has room to distinguish
# "mildly late" from "genuinely stuck" before it maxes out.
HEATMAP_SEVERITY_CAP_SEC = 600
# A cell needs this many delay- or speed-bearing readings to be shown at
# full confidence; fewer readings fade its weight toward 0 (see
# compute_metric_points) rather than being dropped outright.
HEATMAP_CONFIDENT_SAMPLES = 3
# Speed (km/h) at which the speed-heatmap gradient saturates. Set above
# typical arterial running speed for Sydney buses so genuinely fast runs
# (clearways, motorway sections) stand out rather than the whole map
# reading as "hot".
HEATMAP_SPEED_CAP_KMH = 60.0
VALID_HEATMAP_METRICS = ("delay", "density", "speed")

PARSE_YIELD_EVERY = 500
PARSE_YIELD_SEC = 0.001

load_dotenv()
API_KEY = os.getenv("TFNSW_API_KEY")
FEED_URL = os.getenv("TFNSW_GTFS_RT_URL", "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses")
SCHEDULE_URL = os.getenv("TFNSW_GTFS_SCHEDULE_URL", "https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses")
VEHICLE_POS_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"

SCRAPE_REPO = "Joey-Hain/gtfs-r-scrape"
SCRAPE_RAW_BASE = f"https://raw.githubusercontent.com/{SCRAPE_REPO}/main/data"
HEATMAP_WINDOW_CACHE_TTL_SECONDS = 300

# /project page: displays TransportLab's smart-city model config, fetched
# live from GitHub each time the cache goes stale rather than vendored,
# so it always reflects whatever's currently on that repo's main branch.
SMART_CITY_PARAMS_URL = "https://raw.githubusercontent.com/TransportLab/smart-city/main/params.json5"
SMART_CITY_CACHE_TTL_SECONDS = 300

COLOR_FILL = "#00B3F0"
OUTLINE_ON_TIME = "#ffffff"
OUTLINE_LATE = "#B3261E"
OUTLINE_EARLY = "#1E6B3C"
OUTLINE_NO_DATA = "#888888"

SYDNEY_CBD = (-33.8688, 151.2093)
SYDNEY_RADIUS_KM = 10

ENABLE_TRIP_HEADSIGNS = os.getenv("ENABLE_TRIP_HEADSIGNS", "1") == "1"

# How long to wait before retrying a FAILED schedule download. Without this,
# a single transient failure (timeout, temporary OOM, TfNSW hiccup) used to
# be cached forever as "loaded successfully with zero agencies" — this is
# the fix for "headsigns and operator codes missing" persisting indefinitely.
SCHEDULE_RETRY_BACKOFF_SEC = 300


def _log_rss(tag):
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    print(f"[mem] {tag}: rss={kb / 1024:.0f}MB", flush=True)
                    return
    except Exception:
        pass


def haversine_km(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, asin, sqrt
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


def parse_ts(raw):
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC_TZ)
        return ts.astimezone(SYDNEY_TZ)
    except ValueError:
        pass
    try:
        val = float(s)
        if val >= 1e9:
            return datetime.fromtimestamp(val, tz=UTC_TZ).astimezone(SYDNEY_TZ)
    except (ValueError, OSError, OverflowError):
        pass
    return None


app = Flask(__name__)

_schedule_lock = threading.Lock()
_schedule_cache = {"agency_names": None, "trip_headsigns": None, "error": None, "last_attempt": None}


def _download_to_path(url, headers, dest_path, timeout=90):
    total = 0
    with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:
        if r.status_code != 200:
            raise RuntimeError(
                f"Schedule endpoint returned HTTP {r.status_code}. "
                f"API key probably isn't subscribed to the bus schedule product."
            )
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=256 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
                    time.sleep(0)
    return total


def _parse_csv_member(zf, member, key_col, val_col):
    result = {}
    if member not in zf.namelist():
        return result
    with zf.open(member) as raw:
        reader = csv.reader(codecs.iterdecode(raw, "utf-8-sig"))
        try:
            header = [h.strip() for h in next(reader)]
        except StopIteration:
            return result
        if key_col not in header or val_col not in header:
            return result
        key_idx = header.index(key_col)
        val_idx = header.index(val_col)
        for i, row in enumerate(reader):
            if i % PARSE_YIELD_EVERY == 0:
                time.sleep(PARSE_YIELD_SEC)
            if len(row) > max(key_idx, val_idx) and row[val_idx].strip():
                result[row[key_idx].strip()] = row[val_idx].strip()
    return result


def _do_schedule_download():
    tmpdir = tempfile.mkdtemp(prefix="tfnsw_schedule_")
    try:
        outer_path = os.path.join(tmpdir, "schedule.zip")
        size = _download_to_path(SCHEDULE_URL, {"Authorization": f"apikey {API_KEY}"}, outer_path)
        print(f"[schedule] downloaded {size / 1e6:.1f}MB", flush=True)
        _log_rss("after-download")

        agencies = {}
        trip_headsigns = {}

        with zipfile.ZipFile(outer_path) as outer:
            names = outer.namelist()
            if "agency.txt" in names or "trips.txt" in names:
                agencies.update(_parse_csv_member(outer, "agency.txt", "agency_id", "agency_name"))
                if ENABLE_TRIP_HEADSIGNS:
                    trip_headsigns.update(_parse_csv_member(outer, "trips.txt", "trip_id", "trip_headsign"))
            else:
                for i, name in enumerate(names):
                    if not name.endswith(".zip"):
                        continue
                    inner_path = os.path.join(tmpdir, f"inner_{i}.zip")
                    with outer.open(name) as src, open(inner_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=64 * 1024)
                    try:
                        with zipfile.ZipFile(inner_path) as inner:
                            agencies.update(_parse_csv_member(inner, "agency.txt", "agency_id", "agency_name"))
                            if ENABLE_TRIP_HEADSIGNS:
                                trip_headsigns.update(_parse_csv_member(inner, "trips.txt", "trip_id", "trip_headsign"))
                    finally:
                        try:
                            os.unlink(inner_path)
                        except OSError:
                            pass

        print(f"[schedule] parsed {len(agencies)} agencies, {len(trip_headsigns)} headsigns", flush=True)
        _log_rss("after-parse")

        if not agencies:
            raise RuntimeError("Downloaded bundle but no agency rows found")
        return agencies, trip_headsigns, None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def get_schedule_lookups():
    """Return ({agency_id: agency_name}, {trip_id: trip_headsign}, error).

    Downloads and parses the TfNSW schedule bundle on the first call,
    blocking until done. Subsequent successful calls return the cached
    dicts immediately. A lock ensures only one download happens even if
    multiple requests arrive concurrently.

    SUCCESS is determined by truthiness (a non-empty agencies dict), not
    `is not None` — {} is "not None" but is NOT success, and treating it
    as success is what previously caused one failed attempt to permanently
    blank operator names/headsigns. On failure, we retry after
    SCHEDULE_RETRY_BACKOFF_SEC rather than either hammering the endpoint
    on every single request or never retrying again.
    """
    if _schedule_cache["agency_names"]:  # truthy = non-empty dict = genuine success
        return (_schedule_cache["agency_names"],
                _schedule_cache["trip_headsigns"],
                _schedule_cache["error"])

    with _schedule_lock:
        if _schedule_cache["agency_names"]:
            return (_schedule_cache["agency_names"],
                    _schedule_cache["trip_headsigns"],
                    _schedule_cache["error"])

        now = time.monotonic()
        last_attempt = _schedule_cache["last_attempt"]
        if last_attempt is not None and (now - last_attempt) < SCHEDULE_RETRY_BACKOFF_SEC:
            # Still within the backoff window after a previous failure —
            # return the empty state without hammering the endpoint again.
            return {}, {}, _schedule_cache["error"]

        print("[schedule] loading — downloading + parsing, this takes ~30s", flush=True)
        t0 = time.monotonic()
        _schedule_cache["last_attempt"] = now
        try:
            agencies, trip_headsigns, err = _do_schedule_download()
        except Exception as e:
            print(f"[schedule] load failed: {e}", flush=True)
            _schedule_cache["agency_names"] = {}
            _schedule_cache["trip_headsigns"] = {}
            _schedule_cache["error"] = str(e)
            return {}, {}, str(e)

        _schedule_cache["agency_names"] = agencies
        _schedule_cache["trip_headsigns"] = trip_headsigns
        _schedule_cache["error"] = err
        print(f"[schedule] loaded in {time.monotonic() - t0:.1f}s — "
              f"{len(agencies)} agencies, {len(trip_headsigns)} headsigns", flush=True)
        _log_rss("schedule-ready")
        return agencies, trip_headsigns, err


def fetch_feed():
    r = requests.get(FEED_URL, headers={"Authorization": f"apikey {API_KEY}"}, timeout=15)
    r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    return feed


def fetch_vehicle_feed():
    r = requests.get(VEHICLE_POS_URL, headers={"Authorization": f"apikey {API_KEY}"}, timeout=15)
    r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    return feed


def extract_rows(feed, agency_names):
    pulled_at = datetime.now(tz=SYDNEY_TZ).isoformat()
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        agency_id = trip.route_id.split("_")[0] if trip.route_id else "?"
        operator = agency_names.get(agency_id, agency_id)
        for stu in tu.stop_time_update:
            arr_delay = stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None
            dep_delay = stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None
            delay = arr_delay if arr_delay is not None else dep_delay
            if delay is None:
                continue
            rows.append({
                "pulled_at": pulled_at,
                "operator": operator,
                "route_id": trip.route_id,
                "trip_id": trip.trip_id,
                "stop_id": stu.stop_id,
                "stop_sequence": stu.stop_sequence,
                "delay": delay,
                "anomaly": abs(delay) > ANOMALY_ABS_SEC,
            })
    return rows


def latest_reading_per_trip(rows):
    latest = {}
    for r in rows:
        existing = latest.get(r["trip_id"])
        if existing is None or r["stop_sequence"] < existing["stop_sequence"]:
            latest[r["trip_id"]] = r
    return latest


def extract_vehicles(feed, agency_names, trip_headsigns=None):
    trip_headsigns = trip_headsigns or {}
    vehicles = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.HasField("position"):
            continue
        route_id = v.trip.route_id if v.HasField("trip") else ""
        trip_id = v.trip.trip_id if v.HasField("trip") else None
        route_num, route_operator = split_route(route_id, agency_names) if route_id else ("", "")
        vehicles.append({
            "trip_id": trip_id,
            "vehicle_id": v.vehicle.id if v.HasField("vehicle") else entity.id,
            "route_id": route_id,
            "route_num": route_num,
            "route_operator": route_operator,
            "headsign": trip_headsigns.get(trip_id),
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "bearing": v.position.bearing if v.HasField("position") else None,
            "speed": v.position.speed if v.HasField("position") else None,
        })
    return vehicles


def merge_vehicle_delays(vehicles, delay_by_trip):
    for veh in vehicles:
        veh["fill_color"] = COLOR_FILL
        d = delay_by_trip.get(veh["trip_id"])
        if d is None:
            veh["delay_sec"] = None
            veh["delay_min"] = None
            veh["anomaly"] = False
            veh["on_time"] = None
            veh["outline_color"] = OUTLINE_NO_DATA
            continue
        veh["delay_sec"] = d["delay"]
        veh["delay_min"] = round(d["delay"] / 60, 1)
        veh["anomaly"] = d["anomaly"]
        on_time = ON_TIME_EARLY_SEC <= d["delay"] <= ON_TIME_LATE_SEC
        veh["on_time"] = on_time
        if d["anomaly"]:
            veh["outline_color"] = OUTLINE_NO_DATA
        elif on_time:
            veh["outline_color"] = OUTLINE_ON_TIME
        elif d["delay"] > 0:
            veh["outline_color"] = OUTLINE_LATE
        else:
            veh["outline_color"] = OUTLINE_EARLY


def append_to_log(rows):
    file_exists = LOG_FILE.exists()
    with LOG_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["pulled_at", "operator", "route_id", "trip_id", "stop_id", "stop_sequence", "delay", "anomaly"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def summarise(rows, key):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r[key]].append(r["delay"])
    out = []
    for k, delays in grouped.items():
        on_time = sum(1 for d in delays if ON_TIME_EARLY_SEC <= d <= ON_TIME_LATE_SEC)
        out.append({
            key: k,
            "n": len(delays),
            "mean_min": statistics.mean(delays) / 60,
            "stdev_min": (statistics.stdev(delays) if len(delays) > 1 else 0.0) / 60,
            "min_min": min(delays) / 60,
            "max_min": max(delays) / 60,
            "on_time_pct": 100 * on_time / len(delays),
        })
    return out


def split_route(route_id, agency_names):
    if "_" not in route_id:
        return route_id, ""
    agency_id, route_num = route_id.split("_", 1)
    operator = agency_names.get(agency_id, agency_id)
    return route_num, operator


CACHE_TTL_SECONDS = 12
_rows_cache = {"all_rows": None, "agency_names": None, "trip_headsigns": None,
               "agency_error": None, "fetched_at": None}
_vehicles_cache = {"vehicles": None, "fetched_at": None}
# Keyed by window_hours only (NOT by metric) — see the module docstring's
# "HISTORICAL HEATMAP" section for why the fetch/aggregate step is cached
# independently of which metric the caller asked for.
_heatmap_cells_cache = {}

# Locks guarding each cache's fetch path. With threaded gunicorn workers,
# several requests can hit a stale cache at the same instant — without a
# lock, EACH ONE independently triggers its own full fetch+parse (trip-
# update rows, vehicle positions, or heatmap CSVs) at the same time,
# multiplying peak memory by however many threads raced in. This was the
# direct cause of the out-of-memory crash. Double-checked locking: only
# the first thread to acquire the lock actually fetches; everyone else
# re-checks the (now-fresh) cache after acquiring and reuses it.
_rows_lock = threading.Lock()
_vehicles_lock = threading.Lock()
_heatmap_lock = threading.Lock()
_smart_city_cache = {"params": None, "error": None, "fetched_at": None}
_smart_city_lock = threading.Lock()


def _json5_lite_to_json(text):
    """Minimal JSON5 -> JSON rewriter: strips // and /* */ comments, turns
    single-quoted strings into double-quoted, quotes bare object keys, and
    drops trailing commas. Not a full JSON5 parser (no hex numbers, no
    unquoted-string edge cases) but covers everything params.json5 actually
    uses. Comment-stripping and quote-conversion are both string-aware so a
    "//" inside a URL value (e.g. 'https://...') is never mistaken for a
    line comment.
    """

    def strip_comments(s):
        out, i, n, in_str = [], 0, len(s), None
        while i < n:
            c = s[i]
            if in_str:
                out.append(c)
                if c == "\\" and i + 1 < n:
                    out.append(s[i + 1]); i += 2; continue
                if c == in_str:
                    in_str = None
                i += 1; continue
            if c in ("\"", "'"):
                in_str = c; out.append(c); i += 1; continue
            if c == "/" and i + 1 < n and s[i + 1] == "/":
                j = s.find("\n", i); i = n if j == -1 else j; continue
            if c == "/" and i + 1 < n and s[i + 1] == "*":
                j = s.find("*/", i + 2); i = n if j == -1 else j + 2; continue
            out.append(c); i += 1
        return "".join(out)

    def singlequote_to_double(s):
        out, i, n, in_str = [], 0, len(s), None
        while i < n:
            c = s[i]
            if in_str:
                if c == "\\" and i + 1 < n:
                    out.append(c); out.append(s[i + 1]); i += 2; continue
                if c == in_str:
                    out.append('"'); in_str = None; i += 1; continue
                if in_str == "'" and c == '"':
                    out.append('\\"'); i += 1; continue
                out.append(c); i += 1; continue
            if c in ("\"", "'"):
                in_str = c; out.append('"'); i += 1; continue
            out.append(c); i += 1
        return "".join(out)

    t = strip_comments(text)
    t = singlequote_to_double(t)
    t = re.sub(r'([{,]\s*)([A-Za-z_$][A-Za-z0-9_$]*)\s*:', r'\1"\2":', t)
    t = re.sub(r',(\s*[}\]])', r'\1', t)
    return t


def get_smart_city_params_cached():
    now = datetime.now(tz=SYDNEY_TZ)
    fetched_at = _smart_city_cache["fetched_at"]
    if fetched_at is not None and (now - fetched_at).total_seconds() < SMART_CITY_CACHE_TTL_SECONDS:
        return _smart_city_cache["params"], _smart_city_cache["error"]

    with _smart_city_lock:
        now = datetime.now(tz=SYDNEY_TZ)
        fetched_at = _smart_city_cache["fetched_at"]
        if fetched_at is not None and (now - fetched_at).total_seconds() < SMART_CITY_CACHE_TTL_SECONDS:
            return _smart_city_cache["params"], _smart_city_cache["error"]

        params, error = None, None
        try:
            resp = requests.get(SMART_CITY_PARAMS_URL, timeout=15)
            resp.raise_for_status()
            params = json.loads(_json5_lite_to_json(resp.text))
        except requests.RequestException as e:
            error = f"Could not fetch params.json5: {e}"
        except (ValueError, json.JSONDecodeError) as e:
            error = f"Could not parse params.json5: {e}"

        if params is not None or _smart_city_cache["params"] is None:
            _smart_city_cache.update(params=params, error=error, fetched_at=now)
        else:
            # Fetch failed but we have a previously-good copy — keep serving
            # it (with the new error noted) rather than blanking the page.
            _smart_city_cache.update(error=error, fetched_at=now)
        return _smart_city_cache["params"], _smart_city_cache["error"]


PROJECT_SECTION_INFO = {
    "model": ("Model", "Physical dimensions of the projected table model and the real-world lat/lng corners it maps onto."),
    "projector": ("Projector", "Projector resolution, aspect ratio and calibration offsets for the overhead projection."),
    "map": ("Map", "Leaflet map view settings — currently left for the app to calculate from the model properties."),
    "threejs": ("three.js", "Camera placement for the three.js layer drawn over the map."),
    "server": ("Server", "The smart-city app's own backend server settings."),
    "logo": ("Logo", "University logo overlay shown on the model."),
    "gtfs": ("GTFS (buses, light rail, ferries)", "TfNSW GTFS-realtime v1 feed — buses, light rail and ferries."),
    "gtfs2": ("GTFS (metro, trains)", "TfNSW GTFS-realtime v2 feed — Sydney Metro and Sydney Trains."),
    "ais": ("AIS (shipping)", "Live vessel tracking via the aisstream.io AIS feed."),
    "flights": ("Flights", "Live aircraft tracking via the OpenSky Network API."),
    "radar": ("Weather radar", "Bureau of Meteorology rain radar overlay."),
    "hazards": ("Traffic hazards", "TfNSW live hazards feed (roadworks, incidents, closures)."),
}
PROJECT_KEY_LABELS = {
    "ne": "Northeast corner", "sw": "Southwest corner", "lat": "Latitude", "lng": "Longitude",
    "url": "URL", "id": "ID", "ID": "ID", "loc": "Location", "modes": "Modes",
    "show": "Enabled", "opacity": "Opacity", "resolution": "Resolution",
    "aspect_ratio": "Aspect ratio", "vertical_offset": "Vertical offset", "horizontal_scale": "Horizontal scale",
    "update_interval": "Update interval", "throw_distance": "Throw distance", "pixel_size": "Pixel size",
    "camera_location": "Camera location", "camera_rotation": "Camera rotation", "bounds": "Bounds",
}


def _project_label(key):
    return PROJECT_KEY_LABELS.get(key, key.replace("_", " ").replace("-", " ").strip().title())


def _project_format_scalar(key, value):
    if isinstance(value, bool):
        return '<span class="proj-yes">Yes</span>' if value else '<span class="proj-no">No</span>'
    if isinstance(value, float):
        s = f"{value:.6f}".rstrip("0").rstrip(".")
        text = s if s not in ("", "-") else "0"
    else:
        text = str(value)
    if key in ("update_interval",) and isinstance(value, (int, float)):
        return html_escape(f"{text} ms ({value / 1000:g}s)")
    if isinstance(value, str) and re.match(r"^(https?|wss?|ftp)://", value):
        safe = html_escape(value)
        return f'<a href="{safe}" target="_blank" rel="noopener noreferrer">{safe}</a>'
    return html_escape(text)


def render_project_node(value, depth=0):
    if isinstance(value, dict):
        if not value:
            return '<span class="proj-muted">(none)</span>'
        rows = "".join(
            f"<tr><th>{html_escape(_project_label(k))}</th><td>{render_project_node(v, depth + 1)}</td></tr>"
            for k, v in value.items()
        )
        return f'<table class="proj-subtable">{rows}</table>'
    if isinstance(value, list):
        if not value:
            return '<span class="proj-muted">(none)</span>'
        if all(isinstance(x, (int, float, str, bool)) for x in value):
            return ", ".join(_project_format_scalar(None, x) for x in value)
        return "".join(f'<div class="proj-list-item">{render_project_node(x, depth + 1)}</div>' for x in value)
    return _project_format_scalar(None, value)


def render_project_sections(params):
    order = list(PROJECT_SECTION_INFO.keys())
    keys = order + [k for k in params if k not in order]
    cards = []
    for key in keys:
        if key not in params:
            continue
        title, desc = PROJECT_SECTION_INFO.get(key, (_project_label(key), ""))
        body = render_project_node(params[key], depth=0)
        desc_html = f'<div class="proj-card-desc">{html_escape(desc)}</div>' if desc else ""
        cards.append(
            f'<div class="proj-card"><h2>{html_escape(title)}</h2>{desc_html}{body}</div>'
        )
    return "".join(cards)


def get_all_rows_cached():
    now = datetime.now(tz=SYDNEY_TZ)
    cached_at = _rows_cache["fetched_at"]
    if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
        return (_rows_cache["all_rows"], _rows_cache["agency_names"], _rows_cache["trip_headsigns"],
                _rows_cache["agency_error"])

    with _rows_lock:
        now = datetime.now(tz=SYDNEY_TZ)
        cached_at = _rows_cache["fetched_at"]
        if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
            return (_rows_cache["all_rows"], _rows_cache["agency_names"], _rows_cache["trip_headsigns"],
                    _rows_cache["agency_error"])

        agency_names, trip_headsigns, agency_error = get_schedule_lookups()
        feed = fetch_feed()
        all_rows = extract_rows(feed, agency_names)
        if all_rows:
            append_to_log(all_rows)

        _rows_cache.update(all_rows=all_rows, agency_names=agency_names, trip_headsigns=trip_headsigns,
                            agency_error=agency_error, fetched_at=now)
        return all_rows, agency_names, trip_headsigns, agency_error


def get_vehicles_cached(agency_names, trip_headsigns):
    now = datetime.now(tz=SYDNEY_TZ)
    cached_at = _vehicles_cache["fetched_at"]
    if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
        return [dict(v) for v in _vehicles_cache["vehicles"]]

    with _vehicles_lock:
        now = datetime.now(tz=SYDNEY_TZ)
        cached_at = _vehicles_cache["fetched_at"]
        if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
            return [dict(v) for v in _vehicles_cache["vehicles"]]

        vfeed = fetch_vehicle_feed()
        vehicles = extract_vehicles(vfeed, agency_names, trip_headsigns)
        _vehicles_cache.update(vehicles=vehicles, fetched_at=now)
        return [dict(v) for v in vehicles]


def _fetch_one_day_into(date_str, cutoff, local_cells):
    """Aggregate one day's CSV into local_cells, keyed by rounded (lat, lon).

    Each cell accumulates [lat_sum, lon_sum, n_total, late_sum_sec, n_delay,
    speed_sum_kmh, n_speed]:
      - lat_sum/lon_sum/n_total: for the cell's plotted position (its mean
        vehicle location) and its ping count, independent of whether delay
        or speed data was present. n_total alone is what the density metric
        weights by.
      - late_sum_sec/n_delay: for mean lateness, seconds late floored at 0
        (an early or on-time reading contributes 0, never a negative that
        would mask a late reading elsewhere in the same cell). Anomalous
        readings already arrive as an empty delay_sec from the collector,
        so they're naturally excluded here.
      - speed_sum_kmh/n_speed: for mean speed. Zero and missing speed
        readings are common (a bus stopped at a light, or a feed gap) —
        zero is kept (it's a real reading), missing/unparseable is not.
    """
    url = f"{SCRAPE_RAW_BASE}/{date_str}.csv"
    rows_seen = 0
    points_added = 0
    try:
        with requests.get(url, timeout=30, stream=True) as resp:
            print(f"[heatmap] GET {url} -> HTTP {resp.status_code}", flush=True)
            if resp.status_code == 404:
                return date_str, 0, 0, None
            resp.raise_for_status()
            resp.encoding = "utf-8"
            lines = resp.iter_lines(decode_unicode=True)
            try:
                header_line = next(lines)
            except StopIteration:
                return date_str, 0, 0, None
            header = [h.strip() for h in next(csv.reader([header_line]))]
            try:
                ts_idx = header.index("timestamp")
                lat_idx = header.index("lat")
                lon_idx = header.index("lon")
            except ValueError:
                return date_str, 0, 0, f"{date_str}: missing required columns"
            delay_idx = header.index("delay_sec") if "delay_sec" in header else None
            speed_idx = header.index("speed_kmh") if "speed_kmh" in header else None
            max_idx = max(ts_idx, lat_idx, lon_idx)
            for i, row in enumerate(csv.reader(lines)):
                if i % 2000 == 0:
                    time.sleep(PARSE_YIELD_SEC)
                rows_seen += 1
                if len(row) <= max_idx:
                    continue
                ts = parse_ts(row[ts_idx])
                if ts is None or ts < cutoff:
                    continue
                try:
                    lat = float(row[lat_idx])
                    lon = float(row[lon_idx])
                except ValueError:
                    continue
                key = (round(lat, GRID_DECIMALS), round(lon, GRID_DECIMALS))
                c = local_cells[key]
                c[0] += lat
                c[1] += lon
                c[2] += 1
                if delay_idx is not None and len(row) > delay_idx and row[delay_idx].strip():
                    try:
                        delay_sec = float(row[delay_idx])
                    except ValueError:
                        delay_sec = None
                    if delay_sec is not None:
                        c[3] += max(delay_sec, 0.0)
                        c[4] += 1
                if speed_idx is not None and len(row) > speed_idx and row[speed_idx].strip():
                    try:
                        speed_kmh = float(row[speed_idx])
                    except ValueError:
                        speed_kmh = None
                    if speed_kmh is not None and speed_kmh >= 0:
                        c[5] += speed_kmh
                        c[6] += 1
                points_added += 1
            return date_str, rows_seen, points_added, None
    except requests.RequestException as e:
        return date_str, 0, 0, f"{date_str}: {e}"


def fetch_historical_cells(window_hours):
    """Fetch + aggregate a window's CSVs into grid cells. This is the
    expensive, metric-independent half of building a historical heatmap —
    see compute_metric_points() for turning these cells into a specific
    metric's weighted points."""
    start = time.monotonic()
    now = datetime.now(tz=SYDNEY_TZ)
    cutoff = now - timedelta(hours=window_hours)
    dates_needed = []
    d = cutoff.date()
    while d <= now.date():
        dates_needed.append(d.isoformat())
        d += timedelta(days=1)

    workers = max(1, min(HEATMAP_FETCH_CONCURRENCY, len(dates_needed)))
    local_dicts = [defaultdict(lambda: [0.0, 0.0, 0, 0.0, 0, 0.0, 0]) for _ in range(workers)]

    files_fetched = 0
    rows_seen_total = 0
    points_added_total = 0
    last_error = None
    deadline_hit = False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_fetch_one_day_into, ds, cutoff, local_dicts[i % workers])
            for i, ds in enumerate(dates_needed)
        ]
        for future in as_completed(futures):
            if time.monotonic() - start > HEATMAP_DEADLINE_SEC:
                deadline_hit = True
                break
            date_str, rows_seen, points_added, error = future.result()
            if error is not None:
                last_error = error
                continue
            if rows_seen == 0 and points_added == 0:
                continue
            files_fetched += 1
            rows_seen_total += rows_seen
            points_added_total += points_added

    cells = defaultdict(lambda: [0.0, 0.0, 0, 0.0, 0, 0.0, 0])
    for ld in local_dicts:
        for key, c in ld.items():
            tgt = cells[key]
            tgt[0] += c[0]
            tgt[1] += c[1]
            tgt[2] += c[2]
            tgt[3] += c[3]
            tgt[4] += c[4]
            tgt[5] += c[5]
            tgt[6] += c[6]

    cells_with_delay = sum(1 for c in cells.values() if c[4] > 0)
    cells_with_speed = sum(1 for c in cells.values() if c[6] > 0)
    print(f"[heatmap] window={window_hours}h files={files_fetched} "
          f"rows={rows_seen_total} points={points_added_total} cells={len(cells)} "
          f"cells_with_delay={cells_with_delay} cells_with_speed={cells_with_speed} "
          f"elapsed={time.monotonic() - start:.1f}s deadline_hit={deadline_hit}", flush=True)

    return {
        "cells": cells,
        "files_fetched": files_fetched,
        "rows_seen_total": rows_seen_total,
        "last_error": last_error,
        "deadline_hit": deadline_hit,
    }


def compute_metric_points(cells, metric):
    """Turn aggregated cells into [lat, lon, weight] points for one metric.
    Pure/cheap — no network — so switching the metric dropdown client-side
    never re-triggers a CSV fetch (see get_heatmap_points_cached)."""
    if metric == "density":
        # Raw ping count per cell, normalised against the busiest cell in
        # the window. No confidence scaling: the count itself already IS
        # the sample size, unlike the mean-based delay/speed metrics. This
        # is the simplest of the three metrics by design — mainly useful
        # for confirming the heatmap rendering pipeline itself is correct.
        eligible = [c for c in cells.values() if c[2] > 0]
        if not eligible:
            return []
        max_count = max(c[2] for c in eligible)
        return [[c[0] / c[2], c[1] / c[2], c[2] / max_count] for c in eligible]

    if metric == "speed":
        # Weight is mean speed (km/h), normalised against
        # HEATMAP_SPEED_CAP_KMH — hotter = faster, the mirror image of the
        # delay metric. Scaled by the same confidence factor as delay so a
        # single fast/slow ping can't paint a full-strength cell.
        points = []
        for c in cells.values():
            if c[6] <= 0:
                continue
            mean_speed = c[5] / c[6]
            norm = min(mean_speed / HEATMAP_SPEED_CAP_KMH, 1.0)
            confidence = min(c[6] / HEATMAP_CONFIDENT_SAMPLES, 1.0)
            points.append([c[0] / c[2], c[1] / c[2], norm * confidence])
        return points

    # metric == "delay" (default/fallback): mean lateness (seconds late,
    # floored at 0), normalised against HEATMAP_SEVERITY_CAP_SEC — NOT ping
    # density. A cell with plenty of on-time traffic should stay cool; a
    # cell with few but consistently very-late readings should still
    # register as a hotspot. Scaled by the confidence factor so a cell
    # backed by only one or two readings can't paint a full-strength
    # hotspot off a single noisy ping. Cells with zero delay-bearing
    # readings are dropped — there's nothing to weight.
    points = []
    for c in cells.values():
        if c[4] <= 0:
            continue
        severity = min((c[3] / c[4]) / HEATMAP_SEVERITY_CAP_SEC, 1.0)
        confidence = min(c[4] / HEATMAP_CONFIDENT_SAMPLES, 1.0)
        points.append([c[0] / c[2], c[1] / c[2], severity * confidence])
    return points


_METRIC_LABELS = {"delay": "delay", "density": "vehicle", "speed": "speed"}


def get_historical_cells_cached(window_hours):
    now = datetime.now(tz=SYDNEY_TZ)
    cached = _heatmap_cells_cache.get(window_hours)
    if cached is not None and (now - cached["fetched_at"]).total_seconds() < HEATMAP_WINDOW_CACHE_TTL_SECONDS:
        return cached

    with _heatmap_lock:
        now = datetime.now(tz=SYDNEY_TZ)
        cached = _heatmap_cells_cache.get(window_hours)
        if cached is not None and (now - cached["fetched_at"]).total_seconds() < HEATMAP_WINDOW_CACHE_TTL_SECONDS:
            return cached

        meta = fetch_historical_cells(window_hours)
        meta["fetched_at"] = now
        _heatmap_cells_cache[window_hours] = meta
        return meta


def get_heatmap_points_cached(window_hours, metric):
    if metric not in VALID_HEATMAP_METRICS:
        metric = "delay"

    meta = get_historical_cells_cached(window_hours)
    cells = meta["cells"]

    if not cells:
        if meta["files_fetched"] == 0:
            return [], meta["last_error"] or "No data files found for this window"
        return [], f"Fetched {meta['files_fetched']} file(s) but no rows fell inside the window"

    points = compute_metric_points(cells, metric)

    if meta["deadline_hit"]:
        note = f"Partial data (deadline hit after {HEATMAP_DEADLINE_SEC}s)"
    elif not points:
        label = _METRIC_LABELS.get(metric, metric)
        note = f"Fetched {meta['files_fetched']} file(s) but no rows had {label} data yet"
    else:
        note = None
    return points, note


def compute_delay_data(args):
    hide_anomalies = args.get("hide_anomalies", "1") == "1"
    sort_key = args.get("sort", "stdev_min")
    ascending = args.get("asc") == "1"
    q_route = args.get("route", "").strip().lower()
    q_stop = args.get("stop", "").strip().lower()
    q_operator = args.get("operator", "").strip().lower()
    apply_bounds = args.get("bounds", "1") == "1"

    all_rows, agency_names, trip_headsigns, agency_error = get_all_rows_cached()
    rows = [r for r in all_rows if not (hide_anomalies and r["anomaly"])]

    if q_route:
        rows = [r for r in rows if q_route in r["route_id"].lower()]
    if q_operator:
        rows = [r for r in rows if q_operator in r["operator"].lower()]
    if q_stop:
        matching_trip_ids = {r["trip_id"] for r in rows if q_stop in r["stop_id"].lower()}
        rows = [r for r in rows if r["trip_id"] in matching_trip_ids]

    latest_by_trip = latest_reading_per_trip(rows)
    latest_rows = list(latest_by_trip.values())
    delay_by_trip_all = latest_reading_per_trip(all_rows)

    return {
        "hide_anomalies": hide_anomalies, "sort_key": sort_key, "ascending": ascending,
        "q_route": q_route, "q_stop": q_stop, "q_operator": q_operator,
        "agency_names": agency_names, "trip_headsigns": trip_headsigns,
        "agency_error": agency_error, "all_rows": all_rows,
        "latest_by_trip": latest_by_trip, "latest_rows": latest_rows,
        "delay_by_trip_all": delay_by_trip_all,
        "filters_active": bool(q_route or q_stop or q_operator),
        "apply_bounds": apply_bounds,
    }


def compute_vehicles(data):
    try:
        vehicles = get_vehicles_cached(data["agency_names"], data["trip_headsigns"])
    except requests.RequestException as e:
        return [], str(e)
    merge_vehicle_delays(vehicles, data["delay_by_trip_all"])
    if data["filters_active"]:
        allowed = set(data["latest_by_trip"].keys())
        vehicles = [v for v in vehicles if v["trip_id"] in allowed]
    if data["apply_bounds"]:
        lat0, lon0 = SYDNEY_CBD
        vehicles = [v for v in vehicles
                    if v["lat"] is not None and v["lon"] is not None
                    and haversine_km(lat0, lon0, v["lat"], v["lon"]) <= SYDNEY_RADIUS_KM]
    return vehicles, None


PAGE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="robots" content="noindex, nofollow">
<title>TfNSW Delay Board</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
<style>
  :root { --bg:#f7f6f2; --text:#111; --muted:#666; --line:#ccc; --late:#b3261e; --early:#1e6b3c; --fill-blue: {{ color_fill }}; --sans: Helvetica, Arial, sans-serif; }
  * { box-sizing: border-box; }
  body { background:var(--bg); color:var(--text); font-family:var(--sans); margin:0; padding:24px 32px 60px; }
  h1 { font-size:1.4rem; font-weight:bold; border-bottom:2px solid var(--text); padding-bottom:10px; margin-bottom:4px; }
  .meta { color:var(--muted); font-size:0.85rem; margin-bottom:24px; }
  .meta a { color:var(--text); }
  h2 { font-size:1rem; font-weight:bold; margin-top:36px; border-bottom:1px solid var(--line); padding-bottom:4px; }
  table { border-collapse:collapse; width:100%; margin-top:10px; }
  th, td { padding:6px 12px; text-align:right; }
  th:first-child, td:first-child { text-align:left; }
  th { color:var(--muted); font-weight:normal; font-size:0.8rem; border-bottom:1px solid var(--line); }
  th a { color:inherit; text-decoration:underline; }
  th a:hover { color:var(--late); }
  tr:hover { background:#eeece5; }
  td.late { color:var(--late); }
  td.early { color:var(--early); }
  .flag { color:var(--muted); font-size:0.75rem; }
  .toggle { color:var(--text); text-decoration:underline; font-size:0.85rem; }
  #dashmap { height:600px; border:1px solid var(--line); margin-top:10px; background:#e5e3dc; }
  .map-legend { display:flex; gap:16px; align-items:center; font-size:0.8rem; color:var(--muted); margin-top:8px; flex-wrap:wrap; }
  .map-legend .swatch { display:inline-block; width:12px; height:12px; border-radius:50%; margin-right:4px; vertical-align:middle; background:var(--fill-blue); border:2px solid #999; }
  .map-error { color:var(--late); font-size:0.85rem; margin-top:8px; }
  .bus-marker { position:relative; width:56px; height:24px; }
  .bus-pill { position:absolute; top:0; left:50%; transform:translateX(-50%); display:flex; align-items:center; gap:4px; background:var(--fill-blue); color:#fff; font:600 11px/1 -apple-system, Helvetica, Arial, sans-serif; padding:5px 7px; border-radius:7px; border:2.5px solid #888; box-shadow:0 1px 3px rgba(0,0,0,0.4); white-space:nowrap; }
  .bus-arrow { flex:0 0 auto; font-size:12px; line-height:1; display:inline-block; color:#fff; }
  .leaflet-popup-content { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif; }
  .glass-tooltip { background:rgba(255,255,255,0.55) !important; -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45) !important; border-radius:12px !important; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:7px 11px; }
  .glass-tooltip::before { display:none; }
  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper { background:rgba(255,255,255,0.55); -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45); border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; }
  .leaflet-popup.glass-popup .leaflet-popup-tip { background:rgba(255,255,255,0.55); box-shadow:none; }
  .leaflet-control-layers { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif !important; }
  #histWindowPicker { font:12px/1.4 -apple-system, Helvetica, Arial, sans-serif; margin:4px 0 2px 22px; }
  #histWindowPicker select { font:inherit; }
  #histWindowStatus { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#b3261e; margin:2px 0 2px 22px; max-width:220px; }
  .heat-legend { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; margin:6px 22px 2px; color:#333; }
  .heat-legend .heat-legend-title { font-weight:600; margin-bottom:2px; }
  .heat-legend .heat-legend-bar { height:10px; border-radius:2px; border:1px solid rgba(0,0,0,0.15); }
  .heat-legend .heat-legend-ticks { display:flex; justify-content:space-between; color:#888; margin-top:1px; }
  #heatLoadingBanner { position:absolute; top:10px; left:50%; transform:translateX(-50%); z-index:900; background:rgba(17,17,17,0.85); color:#fff; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; box-shadow:0 2px 8px rgba(0,0,0,0.25); pointer-events:none; }
</style>
</head>
<body>
  <h1>Delay Board <a class="toggle" style="float:right; font-size:0.8rem; font-weight:normal; border-bottom:none;" href="/project">smart-city project &rarr;</a></h1>
  <div class="meta">
    Pulled {{ pulled_at }} &middot; {{ n_total }} readings ({{ n_flagged }} flagged as anomalous, {{ 'hidden' if hide_anomalies else 'shown' }})
    &middot; <a class="toggle" href="?hide_anomalies={{ 0 if hide_anomalies else 1 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">{{ 'show anomalies' if hide_anomalies else 'hide anomalies' }}</a>
    <br>"On time" = 1 min early to 5 min late &middot; n = distinct non-anomalous buses reporting (latest stop each)
    {% if agency_error %}<br><span style="color:#b3261e">Operator names unavailable: {{ agency_error }}</span>{% endif %}
  </div>

  <form method="get" style="margin:20px 0; padding:14px; border:1px solid var(--line);">
    <input type="hidden" name="hide_anomalies" value="{{ 1 if hide_anomalies else 0 }}">
    <label>Route <input type="text" name="route" value="{{ q_route }}" placeholder="e.g. 601" style="font-family:inherit;"></label>
    &nbsp;&nbsp;
    <label>Stop ID <input type="text" name="stop" value="{{ q_stop }}" placeholder="e.g. 207618" style="font-family:inherit;"></label>
    &nbsp;&nbsp;
    <label>Operator <input type="text" name="operator" value="{{ q_operator }}" placeholder="e.g. Transdev" style="font-family:inherit;"></label>
    &nbsp;&nbsp;
    <button type="submit" style="font-family:inherit;">Filter</button>
    {% if q_route or q_stop or q_operator %}<a class="toggle" href="?hide_anomalies={{ 1 if hide_anomalies else 0 }}">clear filters</a>{% endif %}
  </form>

  <h2>Live map</h2>
  <div id="dashmap"></div>
  <div class="map-legend">
    <span><span class="swatch" style="border-color:{{ outline_on_time }}"></span>On time</span>
    <span><span class="swatch" style="border-color:{{ outline_late }}"></span>Late</span>
    <span><span class="swatch" style="border-color:{{ outline_early }}"></span>Early</span>
    <span><span class="swatch" style="border-color:{{ outline_no_data }}"></span>No delay data / anomalous</span>
    <span>{{ vehicles|length }} vehicles shown{% if filters_active %} (filtered){% endif %}{% if apply_bounds %} &middot; <a class="toggle" href="?bounds=0&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">within 10km of CBD, show statewide</a>{% else %} &middot; <a class="toggle" href="?bounds=1&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">statewide, restrict to 10km of CBD</a>{% endif %}</span>
    <span>Use the layer switcher (top-right) to toggle the heatmaps.</span>
  </div>
  {% if map_error %}<div class="map-error">Vehicle positions unavailable: {{ map_error }}</div>{% endif %}

  <h2>By operator</h2>
  <table>
    <tr><th>Operator</th><th>n</th><th>avg delay</th><th>spread (&plusmn;min)</th><th>on time</th><th>range</th></tr>
    {% for r in operators %}
    <tr><td>{{ r.operator }}</td><td>{{ r.n }}</td>
      <td class="{{ 'late' if r.mean_min > 0 else 'early' }}">{{ '%+.1f'|format(r.mean_min) }} min</td>
      <td>{{ '%.1f'|format(r.stdev_min) }} min</td>
      <td>{{ '%.0f'|format(r.on_time_pct) }}%</td>
      <td>{{ '%+.1f'|format(r.min_min) }} to {{ '%+.1f'|format(r.max_min) }} min</td></tr>
    {% endfor %}
  </table>

  <h2>By route &mdash; worst variance first (sort: <a class="toggle" href="?sort=stdev_min&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">spread</a> / <a class="toggle" href="?sort=mean_min&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">avg delay</a> / <a class="toggle" href="?sort=on_time_pct&amp;asc=1&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">worst on-time %</a>)</h2>
  <table>
    <tr><th>Route</th><th>Operator</th><th>n</th><th>avg delay</th><th>spread (&plusmn;min)</th><th>on time</th><th>range</th></tr>
    {% for r in routes[:60] %}
    <tr><td>{{ r.route_num }}</td><td>{{ r.route_operator }}</td><td>{{ r.n }}</td>
      <td class="{{ 'late' if r.mean_min > 0 else 'early' }}">{{ '%+.1f'|format(r.mean_min) }} min</td>
      <td>{{ '%.1f'|format(r.stdev_min) }} min</td>
      <td>{{ '%.0f'|format(r.on_time_pct) }}%</td>
      <td>{{ '%+.1f'|format(r.min_min) }} to {{ '%+.1f'|format(r.max_min) }} min</td></tr>
    {% endfor %}
  </table>

  <h2>Individual trips &mdash; largest single delays</h2>
  <table>
    <tr><th>Trip</th><th>Route</th><th>Operator</th><th>Most recent stop</th><th>delay</th></tr>
    {% for r in worst_trips[:30] %}
    <tr><td>{{ r.trip_id }}</td><td>{{ r.route_num }}</td><td>{{ r.route_operator }}</td><td>{{ r.stop_id }}</td>
      <td class="{{ 'late' if r.delay > 0 else 'early' }}">{{ '%+.1f'|format(r.delay / 60) }} min{% if r.anomaly %} <span class="flag">(flagged)</span>{% endif %}</td></tr>
    {% endfor %}
  </table>

  <script>
    const map = L.map('dashmap').setView([-33.8688, 151.2093], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap contributors' }).addTo(map);
    const markers = new Map();

    // One metric dropdown drives both heat layers at once, rather than a
    // separate live+historical toggle pair per metric (6 checkboxes for
    // density/delay/speed × live/historical was unmanageable). Each metric
    // still gets its own live/historical colour-family pair — same idea as
    // the original delay-only scheme — so the two active layers read as
    // distinct from each other, but only one metric's pair is ever visible
    // at a time.
    //
    // Each ramp now has 7 stops instead of 5, spread with more resolution
    // in the upper-middle range (0.55-0.85) rather than jumping straight
    // from "medium" to "darkest" — that jump was most of why hot areas all
    // read as one flat dark blob instead of showing gradation. Paired with
    // HEAT_MAX below (which stops a couple of overlapping points from
    // instantly maxing out the ramp), distinct severities now land on
    // visibly distinct stops instead of all piling onto the top colour.
    // Hue families unchanged from the previous pass (red/violet,
    // blue/orange, green/magenta — each live/hist pair colourblind-safe).
    const METRICS = {
      delay: {
        label: 'Delay',
        liveGradient:  { 0.0:'#f7e9e9', 0.15:'#f0c9c8', 0.35:'#e69795', 0.55:'#dd6664', 0.7:'#cf3d3b', 0.85:'#b21f1d', 1.0:'#7a0f0e' }, // red
        histGradient:  { 0.0:'#eeecf5', 0.15:'#d6d0ea', 0.35:'#b3a7d9', 0.55:'#8f7ec7', 0.7:'#6c58ad', 0.85:'#4c3a8a', 1.0:'#2c2160' }, // violet
        liveTitle: 'Live snapshot — current lateness',
        histTitle: 'Historical window — mean lateness',
        ticks: ['0 min late', 'SEVERITY_CAP+ min late'],
      },
      density: {
        label: 'Density',
        liveGradient:  { 0.0:'#e3eefc', 0.15:'#c2ddf8', 0.35:'#93c1f0', 0.55:'#5da0e3', 0.7:'#2f7fd0', 0.85:'#1a5fa8', 1.0:'#0c3d73' }, // blue
        histGradient:  { 0.0:'#fcece3', 0.15:'#f8d3bd', 0.35:'#f2af86', 0.55:'#ec8a57', 0.7:'#df662f', 0.85:'#b8481a', 1.0:'#7f2f0e' }, // orange
        liveTitle: 'Live snapshot — vehicle density',
        histTitle: 'Historical window — vehicle density',
        ticks: ['fewer pings', 'more pings'],
      },
      speed: {
        label: 'Speed',
        liveGradient:  { 0.0:'#e6f7e6', 0.15:'#c5ecc5', 0.35:'#98d998', 0.55:'#69c069', 0.7:'#3c9e3c', 0.85:'#217a21', 1.0:'#0f4f0f' }, // green
        histGradient:  { 0.0:'#f8e9f0', 0.15:'#f0c8dd', 0.35:'#e498bf', 0.55:'#d669a2', 0.7:'#c2417f', 0.85:'#9c235f', 1.0:'#671041' }, // magenta
        liveTitle: 'Live snapshot — current speed',
        histTitle: 'Historical window — mean speed',
        ticks: ['0 km/h', 'SPEED_CAP+ km/h'],
      },
    };
    const DEFAULT_METRIC = 'delay';
    let currentMetric = DEFAULT_METRIC;
    let lastVehicles = [];

    // Keep these two in sync with their server-side counterparts
    // (HEATMAP_SEVERITY_CAP_SEC, HEATMAP_SPEED_CAP_KMH) so the live and
    // historical legends mean the same thing for the same metric.
    const SEVERITY_CAP_MIN = 10;
    const SPEED_CAP_KMH = 60;
    METRICS.delay.ticks[1] = `${SEVERITY_CAP_MIN}+ min late`;
    METRICS.speed.ticks[1] = `${SPEED_CAP_KMH}+ km/h`;

    const HEAT_RADIUS_M = 220, HEAT_BLUR_M = 200;
    const HEAT_MIN_RADIUS_PX = 12, HEAT_MIN_BLUR_PX = 10;

    function metresToPixels(metres, zoom, lat) {
      const mpp = 156543.03392 * Math.cos(lat * Math.PI / 180) / Math.pow(2, zoom);
      return metres / mpp;
    }
    const markersLayer = L.layerGroup().addTo(map);
    // minOpacity nudged up from the original 0.12 — combined with the old
    // pale gradient stops, low-weight points were nearly invisible against
    // the map background, which was the other half of "switching metric
    // doesn't seem to do anything".
    const HEAT_MIN_OPACITY = 0.22;
    // Leaflet.heat merges nearby points into a shared screen-space grid
    // cell and SUMS their weights before mapping the total through
    // options.max (default 1) to pick a gradient colour — so with max:1,
    // just two or three overlapping vehicles/cells (extremely common
    // anywhere buses share a corridor) instantly saturate to the topmost
    // colour, which is what made hot areas render as one flat dark blob
    // with no visible gradation between "somewhat busy/late" and
    // "extremely busy/late". Raising max gives the ramp headroom: it now
    // takes several stacked full-weight points to reach the darkest
    // colour, so the intermediate stops actually get used.
    const HEAT_MAX = 3.5;
    const heatLayer = L.heatLayer([], { radius:HEAT_MIN_RADIUS_PX, blur:HEAT_MIN_BLUR_PX, max:HEAT_MAX, minOpacity:HEAT_MIN_OPACITY, gradient:METRICS[DEFAULT_METRIC].liveGradient });
    const histHeatLayer = L.heatLayer([], { radius:HEAT_MIN_RADIUS_PX, blur:HEAT_MIN_BLUR_PX, max:HEAT_MAX, minOpacity:HEAT_MIN_OPACITY, gradient:METRICS[DEFAULT_METRIC].histGradient });

    function updateHeatRadii() {
      const zoom = map.getZoom();
      const lat = map.getCenter().lat;
      const radius = Math.max(metresToPixels(HEAT_RADIUS_M, zoom, lat), HEAT_MIN_RADIUS_PX);
      const blur = Math.max(metresToPixels(HEAT_BLUR_M, zoom, lat), HEAT_MIN_BLUR_PX);
      // Leaflet.heat also silently scales every point's weight by
      // 1 / 2^(options.maxZoom - currentZoom) — a "keep the same total
      // heat energy visible regardless of zoom" trick meant for raw
      // point-density heatmaps. Our weights are already a meaningful
      // per-point severity/speed/density value, not a raw count, so that
      // extra scaling just makes colours drift as you zoom (the exact
      // "fidelity isn't preserved when zoomed out" symptom) rather than
      // showing the same data consistently. Pinning maxZoom to the
      // CURRENT zoom on every change forces that scale factor to
      // 2^0 = 1 at all times, so intensity/colour no longer depends on
      // zoom level at all — only radius/blur (real ground distance) does,
      // which is the only zoom-dependence we actually want.
      heatLayer.setOptions({ radius, blur, maxZoom: zoom });
      histHeatLayer.setOptions({ radius, blur, maxZoom: zoom });
    }
    map.on('zoomend', updateHeatRadii);
    updateHeatRadii();

    const LIVE_LAYER_NAME = 'Heatmap (live)';
    const HIST_LAYER_NAME = 'Heatmap (historical)';
    const layersControl = L.control.layers(null, {
      'Bus markers': markersLayer,
      [LIVE_LAYER_NAME]: heatLayer,
      [HIST_LAYER_NAME]: histHeatLayer
    }, { collapsed:false }).addTo(map);

    function gradientCss(gradient) {
      const stops = Object.keys(gradient).sort((a, b) => a - b)
        .map(k => `${gradient[k]} ${Math.round(k * 100)}%`);
      return `linear-gradient(to right, ${stops.join(', ')})`;
    }
    function makeHeatLegend(id) {
      const div = document.createElement('div');
      div.className = 'heat-legend';
      div.id = id;
      div.hidden = true;
      div.innerHTML = `<div class="heat-legend-title"></div>
        <div class="heat-legend-bar"></div>
        <div class="heat-legend-ticks"><span></span><span></span></div>`;
      layersControl.getContainer().appendChild(div);
      return div;
    }
    const liveLegend = makeHeatLegend('liveHeatLegend');
    const histLegend = makeHeatLegend('histHeatLegend');
    map.on('overlayadd', e => {
      if (e.name === LIVE_LAYER_NAME) liveLegend.hidden = false;
      if (e.name === HIST_LAYER_NAME) histLegend.hidden = false;
    });
    map.on('overlayremove', e => {
      if (e.name === LIVE_LAYER_NAME) liveLegend.hidden = true;
      if (e.name === HIST_LAYER_NAME) histLegend.hidden = true;
    });

    function fillLegend(div, gradient, title, ticks) {
      div.querySelector('.heat-legend-title').textContent = title;
      div.querySelector('.heat-legend-bar').style.background = gradientCss(gradient);
      const [t0, t1] = div.querySelectorAll('.heat-legend-ticks span');
      t0.textContent = ticks[0];
      t1.textContent = ticks[1];
    }

    const metricPickerDiv = document.createElement('div');
    metricPickerDiv.id = 'heatMetricPicker';
    metricPickerDiv.innerHTML = `Heatmap metric: <select id="heatMetricSelect">
        <option value="delay" selected>Delay</option>
        <option value="density">Density</option>
        <option value="speed">Speed</option>
      </select>`;
    layersControl.getContainer().appendChild(metricPickerDiv);
    L.DomEvent.disableClickPropagation(metricPickerDiv);

    const pickerDiv = document.createElement('div');
    pickerDiv.id = 'histWindowPicker';
    pickerDiv.innerHTML = `Historical window: <select id="histWindowSelect"><option value="1">Last hour</option><option value="24" selected>Last 24 hours</option><option value="168">Last 7 days</option></select>`;
    layersControl.getContainer().appendChild(pickerDiv);
    const statusDiv = document.createElement('div');
    statusDiv.id = 'histWindowStatus';
    layersControl.getContainer().appendChild(statusDiv);
    L.DomEvent.disableClickPropagation(pickerDiv);

    // Historical fetches can be slow (up to HEATMAP_DEADLINE_SEC on a cold
    // per-window cache — see server docstring) and switching metric or
    // window quickly fires overlapping requests. The old guard only
    // compared the response's metric against currentMetric, so a stale
    // response from a superseded WINDOW change (metric unchanged) could
    // still land and silently overwrite newer data — one of the "switching
    // sometimes doesn't seem to do anything" reports. histRequestSeq
    // tracks the single most recent request regardless of what changed;
    // any response that isn't for the latest request is dropped outright.
    let histRequestSeq = 0;
    const heatLoadingBanner = document.createElement('div');
    heatLoadingBanner.id = 'heatLoadingBanner';
    heatLoadingBanner.hidden = true;
    document.getElementById('dashmap').appendChild(heatLoadingBanner);

    async function loadHistoricalHeatmap() {
      const seq = ++histRequestSeq;
      const wh = document.getElementById('histWindowSelect').value;
      const metricAtRequest = currentMetric;
      statusDiv.textContent = 'Loading…';
      statusDiv.style.color = '#666';
      // Visible on the map itself (not just the collapsed layer control)
      // so a slow cold-cache fetch reads as "still working" rather than
      // "did switching this do anything?".
      heatLoadingBanner.textContent = `Updating ${METRICS[metricAtRequest].label.toLowerCase()} heatmap…`;
      heatLoadingBanner.hidden = false;
      try {
        const res = await fetch(`/api/heatmap?window=${wh}&metric=${metricAtRequest}`);
        const text = await res.text();
        if (seq !== histRequestSeq) return; // superseded by a newer window/metric change
        let data;
        try { data = JSON.parse(text); }
        catch (e) { statusDiv.textContent = `Bad response (HTTP ${res.status})`; statusDiv.style.color = '#b3261e'; return; }
        const points = data.points || [];
        histHeatLayer.setLatLngs(points);
        if (data.error) { statusDiv.textContent = data.error; statusDiv.style.color = '#b3261e'; }
        else if (points.length === 0) { statusDiv.textContent = 'No historical points in this window yet'; statusDiv.style.color = '#b3261e'; }
        else { statusDiv.textContent = points.length + ' historical cells loaded'; statusDiv.style.color = '#666'; }
      } catch (e) {
        if (seq !== histRequestSeq) return;
        statusDiv.textContent = 'Fetch failed: ' + e; statusDiv.style.color = '#b3261e';
      } finally {
        if (seq === histRequestSeq) heatLoadingBanner.hidden = true;
      }
    }
    document.getElementById('histWindowSelect').addEventListener('change', loadHistoricalHeatmap);

    // Per-vehicle live heat weight for the current metric. Returns null to
    // exclude a vehicle from the live layer entirely (no data for that
    // metric), vs 0 which is a real "no heat contribution" reading.
    function liveWeightFor(v, metric) {
      if (metric === 'density') {
        // Every reporting vehicle counts equally — overlapping pings are
        // what create hot spots, via the heat layer's own additive
        // rendering. Deliberately the simplest metric (see server docstring).
        return 1;
      }
      if (metric === 'speed') {
        if (v.speed == null) return null;
        const kmh = v.speed * 3.6;
        if (kmh < 0) return null;
        return Math.min(kmh / SPEED_CAP_KMH, 1);
      }
      // delay (default): weight by this vehicle's own lateness, not by
      // 1-per-vehicle — otherwise the heat layer just draws the route
      // network (wherever buses happen to be) rather than where they're
      // currently running late. Vehicles with no delay reading or a
      // flagged anomaly don't contribute a heat point (still shown as a
      // marker), since we can't say whether they're a bottleneck or not.
      if (v.delay_min == null || v.anomaly) return null;
      return Math.min(Math.max(v.delay_min, 0) / SEVERITY_CAP_MIN, 1);
    }

    function updateLiveHeatFromVehicles(vehicles) {
      const heatPoints = [];
      vehicles.forEach(v => {
        if (v.lat == null || v.lon == null) return;
        const w = liveWeightFor(v, currentMetric);
        if (w !== null) heatPoints.push([v.lat, v.lon, w]);
      });
      heatLayer.setLatLngs(heatPoints);
    }

    function applyMetric(metric) {
      currentMetric = metric;
      const cfg = METRICS[metric];
      heatLayer.setOptions({ gradient: cfg.liveGradient });
      histHeatLayer.setOptions({ gradient: cfg.histGradient });
      // setOptions() is documented to trigger Leaflet.heat's own redraw,
      // but the explicit redraw() calls here are cheap insurance against
      // a canvas that doesn't repaint until the next unrelated map event —
      // exactly what would look like "the dropdown changed but the map
      // didn't" even though the new gradient/data was already applied.
      heatLayer.redraw();
      histHeatLayer.redraw();
      fillLegend(liveLegend, cfg.liveGradient, cfg.liveTitle, cfg.ticks);
      fillLegend(histLegend, cfg.histGradient, cfg.histTitle, cfg.ticks);
      updateLiveHeatFromVehicles(lastVehicles);
      loadHistoricalHeatmap();
    }
    document.getElementById('heatMetricSelect').addEventListener('change', e => applyMetric(e.target.value));

    applyMetric(DEFAULT_METRIC);

    function makeIcon(routeLabel, bearing, outlineColor) {
      const rot = (bearing != null ? bearing : 0) - 90;
      return L.divIcon({ className:'', iconSize:[56,24], iconAnchor:[28,12], popupAnchor:[0,-12],
        html:`<div class="bus-marker"><div class="bus-pill" style="border-color:${outlineColor};"><div class="bus-arrow" style="transform:rotate(${rot}deg);">&#10148;</div><span>${routeLabel}</span></div></div>` });
    }
    function tooltipContent(v) {
      const rl = v.route_num || v.route_id || '?';
      return v.headsign ? `${rl} to ${v.headsign}` : `Route ${rl}`;
    }
    function popupContent(v) {
      const dt = (v.delay_min != null) ? (v.anomaly ? `${v.delay_min > 0 ? '+' : ''}${v.delay_min} min (flagged)` : `${v.delay_min > 0 ? '+' : ''}${v.delay_min} min`) : 'No current delay data';
      const sk = (v.speed != null) ? Math.round(v.speed * 3.6) + ' km/h' : 'Speed unavailable';
      const rl = v.headsign ? `Route ${v.route_num || v.route_id || '?'} to ${v.headsign}` : `Route ${v.route_num || v.route_id || '?'}`;
      return `<strong>${rl}</strong><br>${v.route_operator || 'Unknown operator'}<br>Trip ${v.trip_id ?? '?'}<br>${dt}<br>${sk}`;
    }
    function renderVehicles(vehicles) {
      lastVehicles = vehicles;
      const seen = new Set();
      vehicles.forEach(v => {
        if (v.lat == null || v.lon == null) return;
        const key = v.vehicle_id || v.trip_id;
        seen.add(key);
        const rl = v.route_num || v.route_id || '?';
        const icon = makeIcon(rl, v.bearing, v.outline_color || '#888');
        const popup = popupContent(v);
        const tooltip = tooltipContent(v);
        if (markers.has(key)) {
          const m = markers.get(key);
          m.setLatLng([v.lat, v.lon]); m.setIcon(icon);
          m.getPopup().setContent(popup); m.getTooltip().setContent(tooltip);
        } else {
          const m = L.marker([v.lat, v.lon], { icon }).addTo(markersLayer)
            .bindPopup(popup, { className:'glass-popup' })
            .bindTooltip(tooltip, { direction:'top', offset:[0,-20], className:'glass-tooltip' });
          markers.set(key, m);
        }
      });
      for (const [key, m] of markers) { if (!seen.has(key)) { markersLayer.removeLayer(m); markers.delete(key); } }
      updateLiveHeatFromVehicles(vehicles);
    }
    async function pollVehicles() {
      try { const res = await fetch('/api/vehicles' + window.location.search); const data = await res.json(); renderVehicles(data.vehicles || []); }
      catch (e) { console.warn('Vehicle poll failed', e); }
    }
    renderVehicles({{ vehicles_json|safe }});
    setInterval(pollVehicles, 15000);
    setInterval(loadHistoricalHeatmap, 300000);
  </script>
</body>
</html>
"""


PROJECT_PAGE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="robots" content="noindex, nofollow">
<title>smart-city project &mdash; params.json5</title>
<style>
  :root { --bg:#f7f6f2; --text:#111; --muted:#666; --line:#ccc; --late:#b3261e; --early:#1e6b3c; --sans: Helvetica, Arial, sans-serif; }
  * { box-sizing: border-box; }
  body { background:var(--bg); color:var(--text); font-family:var(--sans); margin:0; padding:24px 32px 60px; }
  h1 { font-size:1.4rem; font-weight:bold; border-bottom:2px solid var(--text); padding-bottom:10px; margin-bottom:4px; }
  .meta { color:var(--muted); font-size:0.85rem; margin-bottom:24px; }
  .meta a, .toggle { color:var(--text); }
  .proj-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(340px, 1fr)); gap:18px; }
  .proj-card { border:1px solid var(--line); background:#fff; padding:14px 16px 16px; }
  .proj-card h2 { font-size:0.95rem; font-weight:bold; margin:0 0 4px; }
  .proj-card-desc { color:var(--muted); font-size:0.78rem; margin-bottom:10px; line-height:1.35; }
  .proj-subtable { width:100%; border-collapse:collapse; font-size:0.85rem; }
  .proj-subtable th, .proj-subtable td { text-align:left; padding:3px 8px 3px 0; vertical-align:top; }
  .proj-subtable th { color:var(--muted); font-weight:normal; white-space:nowrap; width:1%; }
  .proj-subtable .proj-subtable { margin:2px 0; }
  .proj-list-item { border-top:1px solid var(--line); padding-top:4px; margin-top:4px; }
  .proj-list-item:first-child { border-top:none; margin-top:0; padding-top:0; }
  .proj-yes { color:var(--early); }
  .proj-no { color:var(--muted); }
  .proj-muted { color:var(--muted); }
  .proj-error { color:var(--late); border:1px solid var(--late); padding:10px 14px; margin-bottom:20px; font-size:0.85rem; }
  a { color:inherit; }
</style>
</head>
<body>
  <h1>smart-city project <a class="toggle" style="float:right; font-size:0.8rem; font-weight:normal; border-bottom:none;" href="/">&larr; Delay Board</a></h1>
  <div class="meta">
    Live config from <a href="https://github.com/TransportLab/smart-city/blob/main/params.json5" target="_blank" rel="noopener noreferrer">TransportLab/smart-city&nbsp;&middot;&nbsp;params.json5</a>
    &middot; fetched {{ fetched_at }}{% if stale %} (showing last good copy){% endif %}
  </div>
  {% if error %}<div class="proj-error">{{ error }}</div>{% endif %}
  {% if sections_html %}
  <div class="proj-grid">
    {{ sections_html|safe }}
  </div>
  {% elif not error %}
  <p class="proj-muted">No config data available.</p>
  {% endif %}
</body>
</html>
"""


@app.route("/project")
def project():
    params, error = get_smart_city_params_cached()
    fetched_at = _smart_city_cache["fetched_at"]
    fetched_str = fetched_at.strftime("%Y-%m-%d %H:%M:%S %Z") if fetched_at else "never"
    sections_html = render_project_sections(params) if params else ""
    return render_template_string(
        PROJECT_PAGE,
        error=error,
        stale=bool(error and params),
        fetched_at=fetched_str,
        sections_html=sections_html,
    )


@app.route("/ping")
def ping():
    return "pong", 200


@app.route("/health")
def health():
    return "ok", 200


@app.route("/status")
def status():
    heatmap_state = {}
    for wh, entry in _heatmap_cells_cache.items():
        heatmap_state[str(wh)] = {
            "cells": len(entry["cells"]),
            "files_fetched": entry["files_fetched"],
            "error": entry["last_error"],
            "age_seconds": (datetime.now(tz=SYDNEY_TZ) - entry["fetched_at"]).total_seconds(),
        }
    agencies = _schedule_cache["agency_names"]
    headsigns = _schedule_cache["trip_headsigns"]
    return jsonify({
        "schedule_loaded": bool(agencies),
        "schedule_agencies": len(agencies) if agencies else 0,
        "schedule_headsigns": len(headsigns) if headsigns else 0,
        "schedule_error": _schedule_cache["error"],
        "schedule_last_attempt_age_sec": (
            (time.monotonic() - _schedule_cache["last_attempt"])
            if _schedule_cache["last_attempt"] is not None else None
        ),
        "heatmap_cache": heatmap_state,
    })


@app.route("/")
def dashboard():
    if not API_KEY:
        return "TFNSW_API_KEY not set in .env", 500

    data = compute_delay_data(request.args)
    vehicles, map_error = compute_vehicles(data)

    all_rows = data["all_rows"]
    latest_rows = data["latest_rows"]
    agency_names = data["agency_names"]

    # Operator/route-prefix diagnostics used to be dumped straight into the
    # page header for every viewer (schedule-loading was flaky enough during
    # development to want it always visible). That debugging is done now —
    # the same numbers are still available on demand at /status, so the
    # header just shows the one thing a viewer actually needs: whether
    # operator names failed to load at all (agency_error, below).

    operators = sorted(summarise(latest_rows, "operator"), key=lambda r: -abs(r["mean_min"]))
    routes = sorted(summarise(latest_rows, "route_id"),
                    key=lambda r: r[data["sort_key"]] if data["ascending"] else -abs(r[data["sort_key"]]))
    for r in routes:
        r["route_num"], r["route_operator"] = split_route(r["route_id"], agency_names)
    worst_trips = sorted(latest_rows, key=lambda r: -abs(r["delay"]))
    for r in worst_trips:
        r["route_num"], r["route_operator"] = split_route(r["route_id"], agency_names)

    return render_template_string(
        PAGE,
        pulled_at=all_rows[0]["pulled_at"] if all_rows else "-",
        n_total=len(all_rows),
        n_flagged=sum(1 for r in all_rows if r["anomaly"]),
        hide_anomalies=data["hide_anomalies"],
        q_route=data["q_route"], q_stop=data["q_stop"], q_operator=data["q_operator"],
        agency_error=data["agency_error"],
        operators=operators, routes=routes, worst_trips=worst_trips,
        vehicles=vehicles, vehicles_json=json.dumps(vehicles),
        filters_active=data["filters_active"], apply_bounds=data["apply_bounds"],
        map_error=map_error,
        color_fill=COLOR_FILL,
        outline_on_time=OUTLINE_ON_TIME, outline_late=OUTLINE_LATE,
        outline_early=OUTLINE_EARLY, outline_no_data=OUTLINE_NO_DATA,
    )


@app.route("/api/vehicles")
def api_vehicles():
    if not API_KEY:
        return jsonify({"error": "TFNSW_API_KEY not set in .env"}), 500
    data = compute_delay_data(request.args)
    vehicles, map_error = compute_vehicles(data)
    return jsonify({"vehicles": vehicles, "error": map_error})


@app.route("/api/heatmap")
def api_heatmap():
    try:
        try:
            window_hours = int(request.args.get("window", 24))
        except ValueError:
            window_hours = 24
        if window_hours not in (1, 24, 168):
            window_hours = 24
        metric = request.args.get("metric", "delay")
        if metric not in VALID_HEATMAP_METRICS:
            metric = "delay"
        points, error = get_heatmap_points_cached(window_hours, metric)
        return jsonify({"points": points, "window_hours": window_hours, "metric": metric, "error": error})
    except Exception as e:
        return jsonify({"points": [], "window_hours": None, "metric": None, "error": f"Server error: {e}"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)