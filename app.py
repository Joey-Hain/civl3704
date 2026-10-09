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

/api/heatmap takes a &metric= of "delay_mean" (or legacy "delay"),
"delay_median", "delay_std", "frequency", "density", or "speed", and a
&period= of "all" (default), "am_peak", "midday", "pm_peak", or "evening"
(see TIME_PERIODS). Fetching and aggregating a window's CSVs into grid
cells is the expensive part (network + parse), and is identical regardless
of which metric OR period the caller wants — each CSV row's timestamp
determines its period once, during that same aggregation pass, so cells
end up bucketed by period (and duplicated into an "all" bucket) at no extra
fetch cost. That work is cached per window_hours only
(get_historical_cells_cached), independent of both metric and period.
Turning cached cells into a metric's weighted points (compute_metric_points)
is cheap pure arithmetic done fresh on every request, so switching the
metric or period dropdown client-side never triggers a re-fetch — it just
picks a different already-cached bucket of cells.

Per-metric weighting:
    - delay_mean: mean lateness (seconds late, floored at 0 so early running never
    cancels out a late reading elsewhere), not raw ping count — a busy
    interchange with mostly on-time buses should not outrank a quiet
    corridor where buses are consistently 15 minutes late. See
    HEATMAP_SEVERITY_CAP_SEC for the value that saturates the gradient.
    delay_median / delay_std use the same floored-at-0 lateness.
  - frequency: share of a cell's readings that were NOT on time under the
    TfNSW KPI window (ON_TIME_EARLY_SEC..ON_TIME_LATE_SEC) — i.e. 1 minus
    the on-time KPI for that cell. Unlike the delay metrics this uses the
    raw signed delay, so running more than 59s early counts against a cell.
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

Every metric except density also carries a confidence factor —
min(n_readings / HEATMAP_CONFIDENT_SAMPLES, 1) — returned alongside the
value rather than multiplied into it. The client renders these metrics as
an averaged field (FieldLayer): each pixel's colour is the confidence- and
distance-weighted MEAN of nearby cells, and its opacity is how much data
backs it. So a cell backed by one noisy reading fades out instead of
painting a full-strength hotspot, and — unlike the additive heatmap these
metrics used to share with density — a street with many closely packed
readings no longer renders darker just for being busy (Elizabeth St at
~9 km/h used to out-colour the Harbour Bridge at ~42 km/h on speed). A hard minimum-sample cutoff was tried first (for delay) and
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
import gzip
import io
import json
import os
import shutil
import statistics
import tempfile
import threading
import time
import zipfile
from array import array
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template_string, request
from google.transit import gtfs_realtime_pb2

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
UTC_TZ = ZoneInfo("UTC")
DATA_DIR = Path("CIVL3704")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = DATA_DIR / "delay_log.csv"
ANOMALY_ABS_SEC = 3600
# "On time" window, matching the TfNSW bus on-time running KPI: from 59s
# early through 5:59 late, inclusive. A bus a full minute early (-60s) or
# a full 6:00 late (360s) falls outside it, hence -59/359 rather than
# -60/360. Delay is GTFS-R convention: negative = early, positive = late.
ON_TIME_EARLY_SEC = -59
ON_TIME_LATE_SEC = 359

GRID_DECIMALS = 4
# Daily CSVs are DOWNLOADED this many at a time but PARSED one at a time
# into a single set of cells (see fetch_historical_cells) — parallel
# downloads hide GitHub's per-request latency for the 30-day window
# (~31 files of ~200 kB) without each worker holding its own copy of the
# aggregated cells, which is what kept the old parse-in-workers design at 1.
HEATMAP_DOWNLOAD_CONCURRENCY = 6
HEATMAP_DEADLINE_SEC = 20
# Historical windows offered (hours). 720 = 30 days: GitHub's scheduler only
# runs the "every 5 minutes" scrape 3-6 times a day, so 7 days averages
# ~1.3 readings per 10 m cell; 30 days gives the averaged maps ~5x more.
HEATMAP_WINDOWS = (1, 24, 168, 720)
HEATMAP_DEFAULT_WINDOW = 720
# Mean lateness (seconds) at which the heat gradient saturates. Chosen
# well above ON_TIME_LATE_SEC (359s) so the ramp has room to distinguish
# "mildly late" from "genuinely stuck" before it maxes out.
HEATMAP_SEVERITY_CAP_SEC = 600
# A cell needs this many delay- or speed-bearing readings to be shown at
# full confidence; fewer readings fade its weight toward 0 (see
# compute_metric_points) rather than being dropped outright.
HEATMAP_CONFIDENT_SAMPLES = 3
# Speed (km/h) at which the speed-heatmap gradient saturates. 80 km/h =
# the posted limit on the motorway sections buses use (Bradfield Hwy,
# Warringah Fwy, Eastern Distributor), so the scale separates road classes
# instead of maxing out on any arterial clearway. Scrape data (7 days,
# cell means incl. dwell/signal stops): CBD streets ~7-10 km/h, arterials
# ~18-22, motorway sections ~40-45 (medians ~54, p90 ~66).
HEATMAP_SPEED_CAP_KMH = 80.0
VALID_HEATMAP_METRICS = ("delay", "delay_mean", "delay_median", "delay_std", "delay_total",
                         "frequency", "density", "speed")
# Empirical-Bayes shrinkage for the average-delay and not-on-time maps.
# The scrape is sparse (7 days ~ 1.3 readings per 10 m cell; 83% of cells
# hold a single reading), so a lone bus that happened to be 15 min late
# would paint a full-strength hotspot and a one-reading cell's on-time %
# is a coin flip (0% or 100%). Every pixel's estimate is therefore treated
# as if it also held SHRINK_PRIOR_N readings at the network-wide value.
# Applied per PIXEL by the client (FieldLayer), after pooling nearby cells
# — not per cell before pooling, which smooths twice and flattened the map
# to a 1.3-1.9 min band. With pixel-level shrinkage (250 m pooling, ~20
# readings per pixel at the median) the 7-day map keeps real spread:
# average delay p5-p95 0.6-2.9 min, not on time 11-50%.
SHRINK_PRIOR_N = 3
# Colour-scale caps for those two maps, set from that observed spread
# (the old 10 min / 100% caps squeezed everything into the pale end).
DELAY_FIELD_CAP_SEC = 300
NOT_ON_TIME_CAP = 0.6

# Time-of-day buckets for the historical heatmap (from Liha's changes).
# Averaged over a full "Last 24 hours" window, a corridor that's terrible
# for one hour at 17:00 but fine the rest of the day reads as merely
# lukewarm — the bad hour gets diluted by the many fine ones. Splitting the
# window into buckets and letting the query isolate just one lets that kind
# of pattern show up distinctly instead of averaging out. Boundaries are
# Sydney local time (parse_ts already normalises every timestamp to
# SYDNEY_TZ) and deliberately coarse — four buckets, not hourly — so each
# bucket still collects enough readings to clear HEATMAP_CONFIDENT_SAMPLES
# rather than fragmenting into mostly-empty slices. "evening" wraps past
# midnight.
TIME_PERIODS = {
    "am_peak": (6, 10),   # 06:00-10:00
    "midday": (10, 15),   # 10:00-15:00
    "pm_peak": (15, 19),  # 15:00-19:00
    "evening": (19, 6),   # 19:00-06:00, wraps past midnight
}
# "all" is the un-bucketed whole-window view — the original behaviour, and
# still the default.
VALID_HEATMAP_PERIODS = ("all",) + tuple(TIME_PERIODS.keys())
_PERIOD_LABELS = {
    "all": "all-day",
    "am_peak": "AM peak (06:00-10:00)",
    "midday": "midday (10:00-15:00)",
    "pm_peak": "PM peak (15:00-19:00)",
    "evening": "evening (19:00-06:00)",
}


def time_of_day_bucket(ts):
    """Which TIME_PERIODS bucket a Sydney-local timestamp's hour falls
    into. `evening` wraps past midnight (19:00-23:59 and 00:00-05:59)."""
    hour = ts.hour
    for name, (start, end) in TIME_PERIODS.items():
        if start < end:
            if start <= hour < end:
                return name
        elif hour >= start or hour < end:
            return name
    return "evening"  # unreachable given the ranges above; safe fallback


def is_on_time(delay_sec):
    """TfNSW on-time KPI test for a signed delay in seconds."""
    return ON_TIME_EARLY_SEC <= delay_sec <= ON_TIME_LATE_SEC

PARSE_YIELD_EVERY = 500
PARSE_YIELD_SEC = 0.001

load_dotenv()
API_KEY = os.getenv("TFNSW_API_KEY")
FEED_URL = os.getenv("TFNSW_GTFS_RT_URL", "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses")
SCHEDULE_URL = os.getenv("TFNSW_GTFS_SCHEDULE_URL", "https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses")
VEHICLE_POS_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"

SCRAPE_REPO = "Joey-Hain/gtfs-r-scrape"
SCRAPE_RAW_BASE = f"https://raw.githubusercontent.com/{SCRAPE_REPO}/main/data"
# data-local/ = same CSV format, written by gtfs-r-scrape's local_collector.py
# on a home machine (every 15 min) and pushed manually. Kept in a separate
# folder so it never conflicts with the Action's data/ files; each day is
# read from both (a missing file is a cheap 404).
SCRAPE_RAW_BASES = (SCRAPE_RAW_BASE, f"https://raw.githubusercontent.com/{SCRAPE_REPO}/main/data-local")
HEATMAP_WINDOW_CACHE_TTL_SECONDS = 300
# Long windows barely change minute to minute but are the slowest to build
# (a month of 15-min local data is ~1.3M rows), so they're kept longer.
HEATMAP_WINDOW_CACHE_TTL_LONG_SECONDS = 1800

# Route-mask geometry: a de-duplicated bus road network within ~12 km of the
# CBD, built weekly from the TfNSW GTFS shapes.txt by gtfs-r-scrape's
# build_route_shapes.py (the scrape repo holds the API key as a secret, and
# building it there keeps a ~100MB+ schedule parse off this 512MB instance).
# The dashboard fetches it once per page load from /api/route_shapes and
# uses it to clip the heat layers to corridors — see applyRouteMask() in
# HEATMAP_SCRIPT.
ROUTE_SHAPES_URL = f"https://raw.githubusercontent.com/{SCRAPE_REPO}/main/shapes/route_shapes.json"
ROUTE_SHAPES_CACHE_TTL_SECONDS = 6 * 3600

# /project page: a Leaflet view locked to the exact real-world box
# TransportLab's smart-city rig projects onto its physical table model, so
# it can be checked against the physical model for alignment. Hardcoded
# from that project's params.json5 (model.corners.ne / model.corners.sw:
# https://github.com/TransportLab/smart-city/blob/main/params.json5) rather
# than fetched live — Donald only needs these two corners, not the rest of
# that repo's config, and it's one number pair that only changes if the
# physical model itself is rebuilt.
SMART_CITY_CORNER_NE = (-33.83512300658737, 151.27599316594075)
SMART_CITY_CORNER_SW = (-33.894722633376766, 151.13296645445593)

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


# Every row of one scrape snapshot shares a timestamp (~400-900 rows each),
# so parsing each distinct string once saves real time on large files.
_parse_ts_cached = lru_cache(maxsize=8192)(parse_ts)

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
        on_time = is_on_time(d["delay"])
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
        on_time = sum(1 for d in delays if is_on_time(d))
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
# Cached as gzipped bytes: it's served verbatim, and pre-compressing once
# shrinks a few hundred kB of integer JSON to well under a third.
_route_shapes_cache = {"gz": None, "fetched_at": None}
_route_shapes_lock = threading.Lock()


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


def _new_cell():
    """Empty cell accumulator — see _fetch_one_day_into for the layout.
    delay_values is a compact float array rather than a list: every reading
    is now stored twice (its "all" bucket and its time-of-day bucket), and
    a Python float in a list costs ~32 bytes vs 4 here, which matters on a
    512MB instance with a 7-day window."""
    return [0.0, 0.0, 0, 0.0, 0, 0.0, 0, array("f")]


def _new_period_cells():
    return {p: defaultdict(_new_cell) for p in VALID_HEATMAP_PERIODS}


def _add_reading(c, lat, lon, delay_sec, speed_kmh):
    """Fold one CSV row into a single cell accumulator. Shared by every
    period bucket a reading lands in, so "all" and its matching time-of-day
    bucket always agree on how a reading is counted."""
    c[0] += lat
    c[1] += lon
    c[2] += 1
    if delay_sec is not None:
        c[3] += max(delay_sec, 0.0)
        c[4] += 1
        c[7].append(delay_sec)  # raw/signed — floored later where needed
    if speed_kmh is not None and speed_kmh >= 0:
        c[5] += speed_kmh
        c[6] += 1


class _Prefetched:
    """A downloaded daily CSV, shaped like the streaming requests response
    _fetch_one_day_into reads, so parsing is identical either way."""

    def __init__(self, status_code, content):
        self.status_code = status_code
        self._content = content
        self.encoding = "utf-8"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def iter_lines(self, decode_unicode=True):
        return iter(self._content.decode("utf-8").splitlines())


def _download_day(date_str, base=SCRAPE_RAW_BASE):
    """(date_str, _Prefetched or None, error). Network only — safe to run
    in a thread pool."""
    url = f"{base}/{date_str}.csv"
    try:
        r = requests.get(url, timeout=30)
        print(f"[heatmap] GET {url} -> HTTP {r.status_code}", flush=True)
        return date_str, _Prefetched(r.status_code, r.content if r.status_code == 200 else b""), None
    except requests.RequestException as e:
        return date_str, None, f"{date_str}: {e}"


def _fetch_one_day_into(date_str, cutoff, local_cells_by_period, prefetched=None):
    """Aggregate one day's CSV into local_cells_by_period: a dict keyed by
    VALID_HEATMAP_PERIODS ("all" plus each TIME_PERIODS bucket), each value
    a dict-of-cells keyed by rounded (lat, lon). Every row is folded into
    BOTH its "all" cell and its time-of-day cell (time_of_day_bucket(ts)) —
    one pass, no extra fetching.

    Each cell accumulates [lat_sum, lon_sum, n_total, late_sum_sec, n_delay,
    speed_sum_kmh, n_speed, delay_values]:
      - lat_sum/lon_sum/n_total: for the cell's plotted position (its mean
        vehicle location) and its ping count, independent of whether delay
        or speed data was present. n_total alone is what the density metric
        weights by.
      - late_sum_sec/n_delay: for mean lateness, seconds late floored at 0
        (an early or on-time reading contributes 0, never a negative that
        would mask a late reading elsewhere in the same cell). Anomalous
        readings already arrive as an empty delay_sec from the collector,
        so they're naturally excluded here.
      - delay_values: every raw (signed) delay reading. Median/SD floor
        these at 0 when computed (same rule as the mean); the frequency
        metric uses them signed so early running counts against on-time.
      - speed_sum_kmh/n_speed: for mean speed. Zero and missing speed
        readings are common (a bus stopped at a light, or a feed gap) —
        zero is kept (it's a real reading), missing/unparseable is not.
    """
    url = f"{SCRAPE_RAW_BASE}/{date_str}.csv"
    rows_seen = 0
    points_added = 0
    try:
        resp_cm = prefetched if prefetched is not None else requests.get(url, timeout=30, stream=True)
        with resp_cm as resp:
            if prefetched is None:
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
                ts = _parse_ts_cached(row[ts_idx])
                if ts is None or ts < cutoff:
                    continue
                try:
                    lat = float(row[lat_idx])
                    lon = float(row[lon_idx])
                except ValueError:
                    continue
                key = (round(lat, GRID_DECIMALS), round(lon, GRID_DECIMALS))

                delay_sec = None
                if delay_idx is not None and len(row) > delay_idx and row[delay_idx].strip():
                    try:
                        delay_sec = float(row[delay_idx])
                    except ValueError:
                        delay_sec = None
                speed_kmh = None
                if speed_idx is not None and len(row) > speed_idx and row[speed_idx].strip():
                    try:
                        speed_kmh = float(row[speed_idx])
                    except ValueError:
                        speed_kmh = None

                period = time_of_day_bucket(ts)
                _add_reading(local_cells_by_period["all"][key], lat, lon, delay_sec, speed_kmh)
                _add_reading(local_cells_by_period[period][key], lat, lon, delay_sec, speed_kmh)
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

    # Downloads run in parallel; parsing happens here, one file at a time,
    # into a single set of period buckets (a reading lands in "all" and in
    # its time-of-day bucket).
    cells_by_period = _new_period_cells()
    files_fetched = 0
    rows_seen_total = 0
    points_added_total = 0
    last_error = None
    deadline_hit = False

    workers = max(1, min(HEATMAP_DOWNLOAD_CONCURRENCY, len(dates_needed)))
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = [pool.submit(_download_day, ds, base)
                   for ds in dates_needed for base in SCRAPE_RAW_BASES]
        for future in as_completed(futures):
            if time.monotonic() - start > HEATMAP_DEADLINE_SEC:
                deadline_hit = True
                break
            date_str, body, error = future.result()
            if error is None:
                date_str, rows_seen, points_added, error = _fetch_one_day_into(
                    date_str, cutoff, cells_by_period, prefetched=body)
            if error is not None:
                last_error = error
                continue
            if rows_seen == 0 and points_added == 0:
                continue
            files_fetched += 1
            rows_seen_total += rows_seen
            points_added_total += points_added
    finally:
        # Don't block the request on downloads still in flight after a
        # deadline hit; they finish (or time out) in the background.
        pool.shutdown(wait=False, cancel_futures=True)

    cells = cells_by_period["all"]
    cells_with_delay = sum(1 for c in cells.values() if c[4] > 0)
    cells_with_speed = sum(1 for c in cells.values() if c[6] > 0)
    print(f"[heatmap] window={window_hours}h files={files_fetched} "
          f"rows={rows_seen_total} points={points_added_total} cells={len(cells)} "
          f"cells_with_delay={cells_with_delay} cells_with_speed={cells_with_speed} "
          f"by_period=({', '.join(f'{p}={len(cells_by_period[p])}' for p in TIME_PERIODS)}) "
          f"elapsed={time.monotonic() - start:.1f}s deadline_hit={deadline_hit}", flush=True)

    return {
        "cells": cells_by_period,
        "files_fetched": files_fetched,
        "rows_seen_total": rows_seen_total,
        "last_error": last_error,
        "deadline_hit": deadline_hit,
    }


def compute_metric_points(cells, metric):
    """Turn aggregated cells into points for one metric.
    Pure/cheap — no network — so switching the metric dropdown client-side
    never re-triggers a CSV fetch (see get_heatmap_points_cached).

    density -> [lat, lon, weight] for the additive heatmap (Leaflet.heat).
    every other metric -> [lat, lon, value, confidence], value in 0..1
    (the metric normalised against its cap) and confidence =
    min(n / HEATMAP_CONFIDENT_SAMPLES, 1). They're kept separate (not
    multiplied) because the client renders these as an averaged field:
    confidence weights a cell's say in the local mean and sets opacity,
    but never changes the colour — see FieldLayer in HEATMAP_SCRIPT."""
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
            points.append([c[0] / c[2], c[1] / c[2], norm, confidence])
        return points

    if metric == "delay_total":
        # "Delay burden": total lateness accumulated in a cell (bus-seconds
        # late, floored at 0 per reading), additive like density — i.e.
        # where the most bus-minutes of delay pile up across the network.
        # This is roughly what the original summed delay heatmap showed:
        # busy corridors (CBD) rank high because many buses each add a
        # little, which is real total impact, NOT a sign that each bus there
        # runs later.
        #
        # Normalised to the SAME total heat as the density map for this
        # window (sum of count / max_count), redistributed in proportion to
        # each cell's share of total lateness. Raw totals grow with the
        # window (30 days stacks ~15x a day's lateness) and saturated the
        # map everywhere; this keeps it window-independent, and it reads
        # directly against Bus density — an area hotter here than on density
        # carries more than its share of the network's delay.
        eligible = [c for c in cells.values() if c[2] > 0]
        late_cells = [c for c in eligible if c[4] > 0 and c[3] > 0]
        total_late = sum(c[3] for c in late_cells)
        if not late_cells or not total_late:
            return []
        max_count = max(c[2] for c in eligible)
        density_heat = sum(c[2] for c in eligible) / max_count
        scale = density_heat / total_late
        return [[c[0] / c[2], c[1] / c[2], c[3] * scale] for c in late_cells]

    # Delay metrics use lateness in seconds, floored at 0 so early running
    # cannot cancel out late running in the same cell.
    if metric == "frequency":
        # Share of this cell's delay readings that were NOT on time under
        # the TfNSW KPI (59s early .. 5:59 late) — NOT mean lateness. A
        # corridor where buses are often mildly late (frequent, low
        # severity) stays cool on delay_mean but registers here; one
        # extreme outlier among many on-time readings does NOT dominate
        # the way it can on delay_mean. Same confidence scaling as the
        # other metrics. (From Liha's changes; widened from "late only" to
        # "outside the KPI window" so it's exactly 1 - on-time %.)
        # [lat, lon, rate / NOT_ON_TIME_CAP, n_readings] — weight is the raw
        # reading count (not capped confidence) because the client pools
        # these and shrinks per pixel toward metric_prior().
        points = []
        for c in cells.values():
            if c[4] <= 0:
                continue
            off_time = sum(1 for d in c[7] if not is_on_time(d))
            points.append([c[0] / c[2], c[1] / c[2], (off_time / c[4]) / NOT_ON_TIME_CAP, c[4]])
        return points

    if metric in ("delay_median", "delay_std"):
        points = []
        for c in cells.values():
            if c[4] <= 0:
                continue
            late = [max(d, 0.0) for d in c[7]]
            if metric == "delay_median":
                delay_value = statistics.median(late)
            else:
                delay_value = statistics.stdev(late) if c[4] > 1 else 0.0
            severity = min(delay_value / HEATMAP_SEVERITY_CAP_SEC, 1.0)
            confidence = min(c[4] / HEATMAP_CONFIDENT_SAMPLES, 1.0)
            points.append([c[0] / c[2], c[1] / c[2], severity, confidence])
        return points

    # metric == "delay" or "delay_mean" (default/fallback): mean lateness (seconds late,
    # floored at 0), normalised against HEATMAP_SEVERITY_CAP_SEC — NOT ping
    # density. A cell with plenty of on-time traffic should stay cool; a
    # cell with few but consistently very-late readings should still
    # register as a hotspot. Scaled by the confidence factor so a cell
    # backed by only one or two readings can't paint a full-strength
    # hotspot off a single noisy ping. Cells with zero delay-bearing
    # readings are dropped — there's nothing to weight.
    # [lat, lon, mean lateness / DELAY_FIELD_CAP_SEC, n_readings] — same
    # raw-count weighting + client-side per-pixel shrinkage as "frequency".
    # Not clamped here: the client clamps AFTER averaging, so one extreme
    # cell can't be silently capped before it's pooled with its neighbours.
    return [[c[0] / c[2], c[1] / c[2], (c[3] / c[4]) / DELAY_FIELD_CAP_SEC, c[4]]
            for c in cells.values() if c[4] > 0]


def metric_prior(cells, metric):
    """[network value (normalised like the metric's points), SHRINK_PRIOR_N]
    for the metrics the client shrinks per pixel, else None."""
    n = sum(c[4] for c in cells.values())
    if not n:
        return None
    if metric in ("delay", "delay_mean"):
        return [sum(c[3] for c in cells.values()) / n / DELAY_FIELD_CAP_SEC, SHRINK_PRIOR_N]
    if metric == "frequency":
        off = sum(sum(1 for d in c[7] if not is_on_time(d)) for c in cells.values())
        return [off / n / NOT_ON_TIME_CAP, SHRINK_PRIOR_N]
    return None


_METRIC_LABELS = {
    "delay": "delay", "delay_mean": "mean delay", "delay_median": "median delay",
    "delay_std": "delay standard deviation", "delay_total": "delay", "frequency": "on-time",
    "density": "vehicle", "speed": "speed",
}


def get_historical_cells_cached(window_hours):
    now = datetime.now(tz=SYDNEY_TZ)
    cached = _heatmap_cells_cache.get(window_hours)
    ttl = HEATMAP_WINDOW_CACHE_TTL_LONG_SECONDS if window_hours > 168 else HEATMAP_WINDOW_CACHE_TTL_SECONDS
    if cached is not None and (now - cached["fetched_at"]).total_seconds() < ttl:
        return cached

    with _heatmap_lock:
        now = datetime.now(tz=SYDNEY_TZ)
        cached = _heatmap_cells_cache.get(window_hours)
        if cached is not None and (now - cached["fetched_at"]).total_seconds() < ttl:
            return cached

        meta = fetch_historical_cells(window_hours)
        meta["fetched_at"] = now
        _heatmap_cells_cache[window_hours] = meta
        return meta


def get_heatmap_points_cached(window_hours, metric, period="all"):
    if metric not in VALID_HEATMAP_METRICS:
        metric = "delay"
    if period not in VALID_HEATMAP_PERIODS:
        period = "all"

    # The window fetch already bucketed cells by period in its single pass;
    # picking a period here is just a dict lookup, never a fresh fetch.
    meta = get_historical_cells_cached(window_hours)
    cells = meta["cells"].get(period) or {}

    if not cells:
        if meta["files_fetched"] == 0:
            return [], meta["last_error"] or "No data files found for this window", None
        if period == "all":
            return [], f"Fetched {meta['files_fetched']} file(s) but no rows fell inside the window", None
        return [], f"No readings in the {_PERIOD_LABELS.get(period, period)} period for this window", None

    points = compute_metric_points(cells, metric)
    prior = metric_prior(cells, metric)

    if meta["deadline_hit"]:
        note = f"Partial data (deadline hit after {HEATMAP_DEADLINE_SEC}s)"
    elif not points:
        label = _METRIC_LABELS.get(metric, metric)
        note = f"Fetched {meta['files_fetched']} file(s) but no rows had {label} data yet"
    else:
        note = None
    return points, note, prior


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
        # Exact (case-insensitive) name match: the dropdown lists unique
        # operator NAMES, and several operator IDs can share one name
        # (e.g. multiple Transit Systems contracts), so matching by name
        # groups them; exact rather than substring so one name can't also
        # catch another that merely contains it.
        rows = [r for r in rows if r["operator"].lower() == q_operator]
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
        "operator_options": sorted({r["operator"] for r in all_rows if r.get("operator")}, key=str.lower),
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



# The entire map/heatmap/bus-marker script is shared verbatim between the
# dashboard ("/") and the projector view ("/project") — see the /project
# route below for why this is a plain string constant instead of a Jinja
# include: they need to behave identically, and duplicating this by hand
# in two template strings is exactly how earlier rounds drifted (a fix
# applied to one and not the other). Only the single line that creates the
# map (plain interactive view vs. bounds-locked projector view) differs
# between the two pages; everything else — METRICS, HEAT_MAX, the heat
# layer rebuild-on-metric-switch logic, bus markers, popups, polling — is
# this exact text in both.
HEATMAP_SCRIPT = """\
    // Plainer basemap than stock OSM tiles. Was CartoDB Positron
    // (basemaps.cartocdn.com) — CARTO started requiring an API key for
    // that raster endpoint (see docs.carto.com/faqs/carto-basemaps), which
    // would mean an extra secret to provision on Render for a uni project.
    // Esri's "Light Gray Canvas" is the same idea (muted grey, no busy POI
    // icons, heatmap/markers stay the visual focus) but needs no key and
    // has no request quota — split into a Base layer (the grey fill) and a
    // Reference layer on top (just roads/place labels, transparent
    // elsewhere) per Esri's own pairing for this style. If a CARTO look is
    // ever preferred instead, grab a free key at carto.com/basemaps/apikey
    // (5M requests/month free) and add `?key=...` to a cartocdn.com URL.
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
      attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ',
      maxZoom: 16,
    }).addTo(map);
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Reference/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 16,
    }).addTo(map);
    const markers = new Map();

    // One metric dropdown drives both heat layers at once. Each metric gets
    // its own live/historical ramp pair so the two layers stay
    // distinguishable when both are on.
    //
    // Ramps are ColorBrewer sequential schemes: perceptually ordered with
    // monotonic lightness, either one hue family or analogous neighbours
    // (YlGnBu, YlOrRd as "semantic heat") — never a rainbow. Their
    // near-white first stop is trimmed so the low end still reads against
    // the light-grey basemap, and they're interpolated in OKLab (buildLut)
    // rather than sRGB, which is what removes the flat, banded "solid
    // colour" look of the old hand-picked 7-stop gradients.
    const RAMPS = {
      YlOrRd:  ['#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#e31a1c','#bd0026','#800026'],
      PuBu:    ['#d0d1e6','#a6bddb','#74a9cf','#3690c0','#0570b0','#045a8d','#023858'],
      YlGnBu:  ['#edf8b1','#c7e9b4','#7fcdbb','#41b6c4','#1d91c0','#225ea8','#253494','#081d58'],
      RdPu:    ['#fcc5c0','#fa9fb5','#f768a1','#dd3497','#ae017e','#7a0177','#49006a'],
      YlOrBr:  ['#fff7bc','#fee391','#fec44f','#fe9929','#ec7014','#cc4c02','#993404','#662506'],
      BuPu:    ['#bfd3e6','#9ebcda','#8c96c6','#8c6bb1','#88419d','#810f7c','#4d004b'],
      Oranges: ['#fdd0a2','#fdae6b','#fd8d3c','#f16913','#d94801','#a63603','#7f2704'],
      Blues:   ['#c6dbef','#9ecae1','#6baed6','#4292c6','#2171b5','#08519c','#08306b'],
      // Matplotlib "plasma", reversed and with its pale-yellow end dropped
      // (too little contrast on the light basemap). Perceptually uniform,
      // OKLab lightness strictly decreasing 0.79 -> 0.29, but with far more
      // hue travel than a ColorBrewer ramp — that's what makes CBD
      // (amber), arterials (orange), ~40 km/h roads (pink) and motorways
      // (purple) read as clearly different classes on the speed map.
      PlasmaR: ['#fca636','#f2844b','#e16462','#cc4778','#b12a90','#8f0da4','#6a00a8','#41049d','#0d0887'],
    };
    function hexToRgb(h) { const n = parseInt(h.slice(1), 16); return [(n >> 16) & 255, (n >> 8) & 255, n & 255]; }
    const toLin = c => { c /= 255; return c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4); };
    const toSrgb = c => { const v = c <= 0.0031308 ? 12.92 * c : 1.055 * Math.pow(c, 1 / 2.4) - 0.055; return Math.round(Math.min(1, Math.max(0, v)) * 255); };
    function rgbToOklab(rgb) {
      const r = toLin(rgb[0]), g = toLin(rgb[1]), b = toLin(rgb[2]);
      const l = Math.cbrt(0.4122214708*r + 0.5363325363*g + 0.0514459929*b);
      const m = Math.cbrt(0.2119034982*r + 0.6806995451*g + 0.1073969566*b);
      const q = Math.cbrt(0.0883024619*r + 0.2817188376*g + 0.6299787005*b);
      return [0.2104542553*l + 0.7936177850*m - 0.0040720468*q,
              1.9779984951*l - 2.4285922050*m + 0.4505937099*q,
              0.0259040371*l + 0.7827717662*m - 0.8086757660*q];
    }
    function oklabToRgb(lab) {
      const L = lab[0], a = lab[1], b = lab[2];
      const l = Math.pow(L + 0.3963377774*a + 0.2158037573*b, 3);
      const m = Math.pow(L - 0.1055613458*a - 0.0638541728*b, 3);
      const q = Math.pow(L - 0.0894841775*a - 1.2914855480*b, 3);
      return [toSrgb( 4.0767416621*l - 3.3077115913*m + 0.2309699292*q),
              toSrgb(-1.2684380046*l + 2.6097574011*m - 0.3413193965*q),
              toSrgb(-0.0041960863*l - 0.7034186147*m + 1.7076147010*q)];
    }
    // 256-entry RGB lookup table for a ramp, interpolated in OKLab.
    const _lutCache = {};
    function buildLut(name) {
      if (_lutCache[name]) return _lutCache[name];
      const labs = RAMPS[name].map(h => rgbToOklab(hexToRgb(h)));
      const n = labs.length - 1, lut = new Uint8ClampedArray(256 * 3);
      for (let i = 0; i < 256; i++) {
        const t = i / 255 * n, k = Math.min(Math.floor(t), n - 1), f = t - k, A = labs[k], B = labs[k + 1];
        lut.set(oklabToRgb([A[0] + (B[0]-A[0])*f, A[1] + (B[1]-A[1])*f, A[2] + (B[2]-A[2])*f]), i * 3);
      }
      return (_lutCache[name] = lut);
    }
    // Gradient-stop object ({pos: 'rgb(...)'}) sampled from the OKLab LUT,
    // for the legend bar and for Leaflet.heat (density), so both match the
    // field layer's colours exactly.
    function rampStops(name, n = 11) {
      const lut = buildLut(name), o = {};
      for (let i = 0; i < n; i++) {
        const t = i / (n - 1), j = Math.round(t * 255) * 3;
        o[t.toFixed(3)] = `rgb(${lut[j]},${lut[j+1]},${lut[j+2]})`;
      }
      return o;
    }

    // Keep these two in sync with their server-side counterparts
    // (HEATMAP_SEVERITY_CAP_SEC, HEATMAP_SPEED_CAP_KMH) so the live and
    // historical legends mean the same thing for the same metric.
    const SEVERITY_CAP_MIN = 10;  // delay-burden unit (live weight saturates here)
    const DELAY_CAP_MIN = 5;      // average-delay colour scale (DELAY_FIELD_CAP_SEC)
    const SPEED_CAP_KMH = 80;
    const FIELD_RADIUS_M = 160;  // default averaging radius for FieldLayer

    // kind 'field' = averaged value field (FieldLayer) — colour is the
    // local confidence-weighted MEAN of the metric, opacity is how much
    // data backs it, so a busy street can't look worse/faster just by
    // having more readings. kind 'sum' = classic additive heatmap
    // (Leaflet.heat), which is exactly right for density and only density.
    const DELAY_TICKS = ['0', '1', '2', '3', '4', `${DELAY_CAP_MIN}+ min`];
    // radiusM / fullSupport (field metrics only): delay and not-on-time
    // pool over a wider radius and need more data before reaching full
    // opacity, because individual delay readings are noisy and sparse;
    // speed keeps a tighter radius so adjacent roads of different classes
    // (motorway vs the street beside it) don't blend.
    const METRICS = {
      delay: {
        label: 'Average delay', kind: 'field', hist: 'YlOrRd', live: 'PuBu',
        desc: 'How late buses typically run in each area: mean lateness of readings within 250 m (early running counts as 0). Thin data is pulled toward the network average, so one late bus can\u2019t make a hotspot.',
        radiusM: 250, fullSupport: 3,
        liveTitle: 'Live: current delay (min)',
        histTitle: 'Historical: average delay (min)',
        ticks: DELAY_TICKS,
      },
      delay_total: {
        label: 'Delay burden', kind: 'sum', hist: 'RdPu', live: 'PuBu',
        desc: 'Where the most delay accumulates: total bus-minutes late, scaled to the same total as Bus density. Busy corridors rank high even if each bus is only slightly late; areas hotter here than on Bus density carry more than their share of delay.',
        liveTitle: 'Live: buses running late',
        histTitle: 'Historical: total bus-minutes late',
        ticks: ['less', 'more'],
      },
      // % of readings outside the TfNSW on-time KPI window (0:59 early to
      // 5:59 late) — i.e. 1 − on-time %. From Liha's changes.
      frequency: {
        label: 'Not on time', kind: 'field', hist: 'YlOrBr', live: 'BuPu',
        desc: 'Share of readings within 250 m outside the TfNSW on-time window (0:59 early to 5:59 late), so running early counts too. Thin data is pulled toward the network average.',
        radiusM: 250, fullSupport: 3,
        liveTitle: 'Live: currently not on time',
        histTitle: 'Historical: readings not on time (%)',
        ticks: ['0', '15', '30', '45', '60+%'],
      },
      speed: {
        label: 'Speed', kind: 'field', hist: 'PlasmaR', live: 'YlGnBu',
        desc: 'Average bus speed within 160 m, including time stopped at stops and signals. Amber = CBD streets, pink = arterials, purple = motorways.',
        radiusM: FIELD_RADIUS_M, fullSupport: 0.4,
        liveTitle: 'Live: current speed (km/h)',
        histTitle: 'Historical: average speed (km/h)',
        ticks: ['0', '20', '40', '60', `${SPEED_CAP_KMH}+`],
      },
      density: {
        label: 'Bus density', kind: 'sum', hist: 'Oranges', live: 'Blues',
        desc: 'How many bus position readings were recorded in each area: where buses run most, not how well they run.',
        liveTitle: 'Live: bus density',
        histTitle: 'Historical: bus density',
        ticks: ['fewer buses', 'more buses'],
      },
    };
    for (const cfg of Object.values(METRICS)) {
      cfg.liveGradient = rampStops(cfg.live);
      cfg.histGradient = rampStops(cfg.hist);
    }
    const DEFAULT_METRIC = 'delay';
    let currentMetric = DEFAULT_METRIC;
    let lastVehicles = [];
    // 'pill' = the original route-labelled marker; 'arrow' = just the
    // outline-coloured, bearing-rotated arrowhead with no label/background,
    // for a less cluttered view when many buses are on screen at once.
    let markerStyle = 'pill';

    const HEAT_RADIUS_M = 220, HEAT_BLUR_M = 200;
    // Kept small — this is only a floor so a point never shrinks to an
    // invisible sub-pixel dot at extreme zoom-out, not a target radius.
    // It used to be 12/10px, which is easily *larger* than the true
    // ground-accurate radius once you're zoomed out a few levels, so
    // nearby cells were forced to blend together well before they
    // geographically should have — a real (if separate from the
    // maxZoom-damping fix below) cause of "loses specificity when
    // zoomed out". Lower floor keeps points true-to-scale for longer.
    const HEAT_MIN_RADIUS_PX = 4, HEAT_MIN_BLUR_PX = 3;

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
    let currentHeatRadius = HEAT_MIN_RADIUS_PX, currentHeatBlur = HEAT_MIN_BLUR_PX;

    // heatLayer/histHeatLayer are rebuilt from scratch (not mutated via
    // setOptions) whenever the metric changes — see swapHeatLayer() below.
    // Relying on setOptions()+redraw() to pick up a new gradient on an
    // already-initialised Leaflet.heat layer turned out not to reliably
    // repaint in every case (this was Donald's "switching metric still
    // doesn't visibly change anything" report even after the setOptions
    // patch). Constructing a brand-new layer and running it through
    // Leaflet's normal add/remove lifecycle is the one code path that's
    // guaranteed to (re)initialise the canvas with the new options, so
    // metric switches no longer depend on a third-party plugin's internal
    // caching behaviour at all.
    // ---- Route mask: clip both heat layers to bus corridors ----
    // The heat layers draw round blobs (HEAT_RADIUS_M) around each cell,
    // which smears delay across blocks no bus ever drives through. With
    // the mask on, after Leaflet.heat paints its canvas we stroke the bus
    // road network (from /api/route_shapes — TfNSW GTFS shapes, built
    // weekly by gtfs-r-scrape) at ±routeMaskHalfWidthM using the
    // 'destination-in' composite: heat survives only where a route line
    // was drawn, so hot areas read as corridors. Line width is set in
    // real metres per redraw, so the corridor stays the same ground width
    // at every zoom. If the shapes file isn't available the mask is
    // simply skipped and the heatmap renders unclipped, as before.
    let routeLines = null;
    let routeMaskHalfWidthM = 25;  // 0 = off
    function decodeRouteLines(lines) {
      // Each line is flat [dlat, dlon, ...] in 1e-5 degrees, delta-encoded.
      return lines.map(enc => {
        const pts = new Float64Array(enc.length);
        let lat = 0, lon = 0, s = 90, n = -90, w = 180, e = -180;
        for (let i = 0; i < enc.length; i += 2) {
          lat += enc[i]; lon += enc[i + 1];
          const la = lat / 1e5, lo = lon / 1e5;
          pts[i] = la; pts[i + 1] = lo;
          if (la < s) s = la;
          if (la > n) n = la;
          if (lo < w) w = lo;
          if (lo > e) e = lo;
        }
        return { pts, s, n, w, e };
      });
    }
    function applyRouteMask(layer) {
      if (!routeLines || routeMaskHalfWidthM <= 0 || !layer._map || !layer._canvas) return;
      const m = layer._map;
      const canvas = layer._canvas;
      const ctx = canvas.getContext('2d');
      const b = m.getBounds().pad(0.1);
      const S = b.getSouth(), N = b.getNorth(), W = b.getWest(), E = b.getEast();
      ctx.save();
      // simpleheat leaves globalAlpha at its last point's opacity (often
      // minOpacity) — reset it, or the mask stroke itself is translucent
      // and 'destination-in' fades the corridors it's meant to keep.
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = 'destination-in';
      ctx.lineWidth = Math.max(metresToPixels(2 * routeMaskHalfWidthM, m.getZoom(), m.getCenter().lat), 2);
      ctx.lineCap = 'round';
      ctx.lineJoin = 'round';
      ctx.strokeStyle = '#000';
      ctx.beginPath();
      let drawn = 0;
      for (const ln of routeLines) {
        if (ln.n < S || ln.s > N || ln.e < W || ln.w > E) continue;  // off-screen
        const p = ln.pts;
        // Leaflet.heat's canvas is pinned to container pixel (0,0) and
        // plots its own points with latLngToContainerPoint — same here.
        let pt = m.latLngToContainerPoint([p[0], p[1]]);
        ctx.moveTo(pt.x, pt.y);
        for (let i = 2; i < p.length; i += 2) {
          pt = m.latLngToContainerPoint([p[i], p[i + 1]]);
          ctx.lineTo(pt.x, pt.y);
        }
        drawn++;
      }
      // No route in view means nothing should survive the mask.
      if (drawn) ctx.stroke(); else ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.restore();
    }
    const MaskedHeatLayer = L.HeatLayer.extend({
      // L.latLng() only accepts 2- or 3-element arrays (returns null for a
      // 4th element), so field-metric points [lat, lon, value, confidence]
      // carried over during a metric switch would crash Leaflet.heat.
      setLatLngs(pts) {
        return L.HeatLayer.prototype.setLatLngs.call(this, (pts || []).map(p => p.length > 3 ? [p[0], p[1], p[2]] : p));
      },
      _redraw() {
        L.HeatLayer.prototype._redraw.call(this);
        applyRouteMask(this);
      },
    });

    // ---- Averaged value field (all non-density metrics) ----
    // Leaflet.heat ADDS overlapping points before colouring, so on a value
    // metric (speed, delay) a street with many closely spaced readings
    // stacks up to the darkest colour regardless of the values themselves:
    // Elizabeth St (mean ~9 km/h, ~200 tightly packed cells) rendered darker
    // than the Harbour Bridge (~42 km/h, one reading per cell) on the speed
    // map, and the delay maps had the same bias toward busy streets.
    //
    // FieldLayer instead computes, per pixel, a Gaussian-weighted MEAN of
    // nearby cells (normalised convolution): colour = sum(k*c*v) / sum(k*c),
    // where v is the cell's normalised value, c its confidence (sample
    // count / HEATMAP_CONFIDENT_SAMPLES, capped at 1) and k the kernel.
    // Opacity comes from sum(k*c) — how much data backs that pixel — so
    // thin evidence fades out instead of being painted at full strength,
    // but it never changes the colour. Rendered on a coarse grid (~6 cells
    // per kernel radius) and upscaled with smoothing, so cost stays flat
    // across zoom levels.
    const FieldLayer = L.Layer.extend({
      // fullSupport: summed kernel*confidence at which a pixel reaches
      // maxOpacity. 0.4 ~ one or two readings at the pixel; most cells only
      // have one, and a higher threshold left the whole map washed out.
      options: { radiusM: FIELD_RADIUS_M, maxOpacity: 0.95, fullSupport: 0.4, ramp: 'YlOrRd' },
      initialize(points, options) {
        L.setOptions(this, options);
        this._pts = points || [];
        this._lut = buildLut(this.options.ramp);
      },
      setLatLngs(points) { this._pts = points || []; return this.redraw(); },
      // [value, n] network prior from /api/heatmap (see SHRINK_PRIOR_N),
      // or null: each pixel's mean is shrunk as if it also held n readings
      // at `value`. Opacity still comes from the real data only.
      setPrior(prior) { this._prior = prior || null; return this; },
      // Leaflet.heat API parity: updateHeatRadii() passes radius/blur/maxZoom
      // to every heat layer on zoom; none apply here (moveend redraws).
      setOptions(o) { L.setOptions(this, o); return this; },
      redraw() {
        if (this._map && !this._frame && !this._map._animating) {
          this._frame = L.Util.requestAnimFrame(this._redraw, this);
        }
        return this;
      },
      onAdd(map) {
        this._map = map;
        const c = this._canvas = L.DomUtil.create('canvas', 'leaflet-heatmap-layer leaflet-layer');
        const animated = map.options.zoomAnimation && L.Browser.any3d;
        L.DomUtil.addClass(c, 'leaflet-zoom-' + (animated ? 'animated' : 'hide'));
        map.getPanes().overlayPane.appendChild(c);
        map.on('moveend', this._reset, this);
        if (animated) map.on('zoomanim', this._animateZoom, this);
        this._reset();
      },
      onRemove(map) {
        map.getPanes().overlayPane.removeChild(this._canvas);
        map.off('moveend', this._reset, this);
        map.off('zoomanim', this._animateZoom, this);
        if (this._frame) { L.Util.cancelAnimFrame(this._frame); this._frame = null; }
      },
      _reset() {
        L.DomUtil.setPosition(this._canvas, this._map.containerPointToLayerPoint([0, 0]));
        const size = this._map.getSize();
        if (this._canvas.width !== size.x) this._canvas.width = size.x;
        if (this._canvas.height !== size.y) this._canvas.height = size.y;
        this._redraw();
      },
      _animateZoom(e) {
        const scale = this._map.getZoomScale(e.zoom);
        const offset = this._map._getCenterOffset(e.center)._multiplyBy(-scale).subtract(this._map._getMapPanePos());
        L.DomUtil.setTransform(this._canvas, offset, scale);
      },
      _redraw() {
        this._frame = null;
        const m = this._map;
        if (!m || !this._canvas) return;
        const canvas = this._canvas, ctx = canvas.getContext('2d');
        const W = canvas.width, H = canvas.height;
        ctx.clearRect(0, 0, W, H);
        if (!this._pts.length) return;

        const R = Math.max(metresToPixels(this.options.radiusM, m.getZoom(), m.getCenter().lat), 3);
        const ds = Math.max(1, R / 6);                 // grid cell size (px)
        const gw = Math.ceil(W / ds) + 1, gh = Math.ceil(H / ds) + 1;
        const r = R / ds, r2 = r * r, inv2s2 = 1 / (2 * (r / 2) * (r / 2));  // sigma = R/2, cut at R
        const num = new Float32Array(gw * gh), den = new Float32Array(gw * gh);
        for (const p of this._pts) {
          const c = p.length > 3 ? p[3] : 1;
          if (!(c > 0)) continue;
          const pt = m.latLngToContainerPoint([p[0], p[1]]);
          const gx = pt.x / ds, gy = pt.y / ds;
          if (gx < -r || gy < -r || gx > gw + r || gy > gh + r) continue;
          const v = p[2];  // clamped after averaging, not before
          const x0 = Math.max(0, Math.ceil(gx - r)), x1 = Math.min(gw - 1, Math.floor(gx + r));
          const y0 = Math.max(0, Math.ceil(gy - r)), y1 = Math.min(gh - 1, Math.floor(gy + r));
          for (let y = y0; y <= y1; y++) {
            const dy = y - gy, row = y * gw;
            for (let x = x0; x <= x1; x++) {
              const dx = x - gx, d2 = dx * dx + dy * dy;
              if (d2 > r2) continue;
              const k = Math.exp(-d2 * inv2s2) * c;
              num[row + x] += k * v;
              den[row + x] += k;
            }
          }
        }

        // (not `_off` — that's an internal L.Evented method name)
        const off = this._gridCanvas || (this._gridCanvas = document.createElement('canvas'));
        off.width = gw; off.height = gh;
        const octx = off.getContext('2d');
        const img = octx.createImageData(gw, gh), d = img.data, lut = this._lut;
        const full = this.options.fullSupport, maxA = this.options.maxOpacity * 255;
        const pv = this._prior ? this._prior[0] : 0, pn = this._prior ? this._prior[1] : 0;
        for (let i = 0; i < gw * gh; i++) {
          const w = den[i];
          if (w < 1e-3) continue;
          const val = (num[i] + pn * pv) / (w + pn);
          const li = Math.round(Math.min(Math.max(val, 0), 1) * 255) * 3, j = i * 4;
          d[j] = lut[li]; d[j + 1] = lut[li + 1]; d[j + 2] = lut[li + 2];
          d[j + 3] = Math.min(1, w / full) * maxA;
        }
        octx.putImageData(img, 0, 0);
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = 'high';
        ctx.drawImage(off, -ds / 2, -ds / 2, gw * ds, gh * ds);  // grid cell i is centred on pixel i*ds
        applyRouteMask(this);
      },
    });

    function buildHeatLayer(metric, which) {
      const cfg = METRICS[metric];
      if (cfg.kind === 'field') {
        return new FieldLayer([], {
          ramp: cfg[which],
          radiusM: cfg.radiusM || FIELD_RADIUS_M,
          // Live = one point per bus, nothing pooled — full opacity at ~1 bus.
          fullSupport: which === 'live' ? Math.min(cfg.fullSupport || 0.4, 1) : (cfg.fullSupport || 0.4),
        });
      }
      return new MaskedHeatLayer([], {
        radius: currentHeatRadius, blur: currentHeatBlur, max: HEAT_MAX,
        minOpacity: HEAT_MIN_OPACITY, maxZoom: map.getZoom(),
        gradient: which === 'live' ? cfg.liveGradient : cfg.histGradient,
      });
    }
    let heatLayer = buildHeatLayer(DEFAULT_METRIC, 'live');
    let histHeatLayer = buildHeatLayer(DEFAULT_METRIC, 'hist');
    let liveHeatData = [];
    let histHeatData = [];

    function updateHeatRadii() {
      const zoom = map.getZoom();
      const lat = map.getCenter().lat;
      currentHeatRadius = Math.max(metresToPixels(HEAT_RADIUS_M, zoom, lat), HEAT_MIN_RADIUS_PX);
      currentHeatBlur = Math.max(metresToPixels(HEAT_BLUR_M, zoom, lat), HEAT_MIN_BLUR_PX);
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
      heatLayer.setOptions({ radius: currentHeatRadius, blur: currentHeatBlur, maxZoom: zoom });
      histHeatLayer.setOptions({ radius: currentHeatRadius, blur: currentHeatBlur, maxZoom: zoom });
    }
    map.on('zoomend', updateHeatRadii);
    updateHeatRadii();

    const LIVE_LAYER_NAME = 'Live heatmap';
    const HIST_LAYER_NAME = 'Historical heatmap';
    const layersControl = L.control.layers(null, {
      'Bus markers': markersLayer,
      [LIVE_LAYER_NAME]: heatLayer,
      [HIST_LAYER_NAME]: histHeatLayer
    }, { collapsed:false }).addTo(map);

    // Swap `oldLayer` out for a freshly-built one for `metric`,
    // carrying over its current data and visibility (checked/unchecked in
    // the layer control) and keeping the control's own bookkeeping in
    // sync. Returns the new layer — callers must reassign their
    // heatLayer/histHeatLayer binding to it.
    function swapHeatLayer(oldLayer, metric, which, layerName, currentData) {
      const wasVisible = map.hasLayer(oldLayer);
      const newLayer = buildHeatLayer(metric, which);
      newLayer.setLatLngs(currentData);
      layersControl.removeLayer(oldLayer);
      if (wasVisible) map.removeLayer(oldLayer);
      if (wasVisible) newLayer.addTo(map);
      layersControl.addOverlay(newLayer, layerName);
      return newLayer;
    }

    function gradientCss(gradient) {
      const stops = Object.keys(gradient).sort((a, b) => a - b)
        .map(k => `${gradient[k]} ${Math.round(k * 100)}%`);
      return `linear-gradient(to right, ${stops.join(', ')})`;
    }
    // One tidy panel under the layer checkboxes: a Heatmap section (metric,
    // route clip, history window, time of day, then legend + status) and a
    // Markers section. Element IDs are unchanged so the handlers below
    // don't care how the panel is laid out.
    const PERIODS = [  // Liha's time-of-day buckets — see TIME_PERIODS server-side
      ['all', 'All day'],
      ['am_peak', 'AM peak (06:00\u201310:00)'],
      ['midday', 'Midday (10:00\u201315:00)'],
      ['pm_peak', 'PM peak (15:00\u201319:00)'],
      ['evening', 'Evening (19:00\u201306:00)'],
    ];
    const heatPanel = document.createElement('div');
    heatPanel.className = 'heat-panel';
    heatPanel.innerHTML = `
      <div class="hp-section">Heatmap</div>
      <div class="hp-grid">
        <label for="heatMetricSelect">Metric</label>
        <select id="heatMetricSelect">
          ${Object.entries(METRICS).map(([k, c]) => `<option value="${k}"${k === DEFAULT_METRIC ? ' selected' : ''}>${c.label}</option>`).join('')}
        </select>
        <label for="routeMaskSelect">Route clip</label>
        <span class="hp-inline">
          <select id="routeMaskSelect">
            <option value="0">Off</option>
            <option value="15">\u00b115 m</option>
            <option value="25" selected>\u00b125 m</option>
            <option value="50">\u00b150 m</option>
            <option value="100">\u00b1100 m</option>
          </select>
          <span id="routeMaskStatus">loading\u2026</span>
        </span>
        <label for="histWindowSelect">History</label>
        <select id="histWindowSelect">
          <option value="1">Last hour</option>
          <option value="24">Last 24 hours</option>
          <option value="168">Last 7 days</option>
          <option value="720" selected>Last 30 days</option>
        </select>
        <label for="histPeriodSelect">Time of day</label>
        <select id="histPeriodSelect">
          ${PERIODS.map(([v, label]) => `<option value="${v}"${v === 'all' ? ' selected' : ''}>${label}</option>`).join('')}
        </select>
      </div>
      <div class="hp-legends"></div>
      <div id="heatMetricDesc" class="hp-desc"></div>
      <div id="histWindowStatus"></div>
      <div class="hp-section">Markers</div>
      <div class="hp-grid">
        <label for="markerStyleSelect">Style</label>
        <select id="markerStyleSelect">
          <option value="pill" selected>Route label</option>
          <option value="arrow">Arrow only</option>
        </select>
      </div>`;
    layersControl.getContainer().appendChild(heatPanel);
    L.DomEvent.disableClickPropagation(heatPanel);
    L.DomEvent.disableScrollPropagation(heatPanel);
    const statusDiv = heatPanel.querySelector('#histWindowStatus');

    function makeHeatLegend(id) {
      const div = document.createElement('div');
      div.className = 'heat-legend';
      div.id = id;
      div.hidden = true;
      div.innerHTML = `<div class="heat-legend-title"></div>
        <div class="heat-legend-bar"></div>
        <div class="heat-legend-ticks"></div>`;
      heatPanel.querySelector('.hp-legends').appendChild(div);
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
      const tickRow = div.querySelector('.heat-legend-ticks');
      tickRow.replaceChildren(...ticks.map(t => {
        const span = document.createElement('span');
        span.textContent = t;
        return span;
      }));
    }

    document.getElementById('routeMaskSelect').addEventListener('change', e => {
      routeMaskHalfWidthM = Number(e.target.value);
      heatLayer.redraw();
      histHeatLayer.redraw();
    });
    async function loadRouteShapes() {
      const st = document.getElementById('routeMaskStatus');
      try {
        const res = await fetch('/api/route_shapes');
        const data = await res.json();
        if (!data.lines || !data.lines.length) {
          st.textContent = 'unavailable';
          st.title = data.error || 'No route shapes';
          return;
        }
        routeLines = decodeRouteLines(data.lines);
        st.textContent = '';
        st.title = '';
        heatLayer.redraw();
        histHeatLayer.redraw();
      } catch (e) {
        st.textContent = 'unavailable';
        st.title = String(e);
      }
    }

    document.getElementById('markerStyleSelect').addEventListener('change', e => {
      markerStyle = e.target.value;
      renderVehicles(lastVehicles);
    });

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
      const period = document.getElementById('histPeriodSelect').value;
      const metricAtRequest = currentMetric;
      statusDiv.textContent = 'Loading…';
      statusDiv.style.color = '#666';
      // Visible on the map itself (not just the collapsed layer control)
      // so a slow cold-cache fetch reads as "still working" rather than
      // "did switching this do anything?".
      heatLoadingBanner.textContent = `Updating heatmap: ${METRICS[metricAtRequest].label.toLowerCase()}\u2026`;
      heatLoadingBanner.hidden = false;
      try {
        const res = await fetch(`/api/heatmap?window=${wh}&metric=${metricAtRequest}&period=${period}`);
        const text = await res.text();
        if (seq !== histRequestSeq) return; // superseded by a newer window/metric change
        let data;
        try { data = JSON.parse(text); }
        catch (e) { statusDiv.textContent = `Bad response (HTTP ${res.status})`; statusDiv.style.color = '#b3261e'; return; }
        const points = data.points || [];
        histHeatData = points;
        if (histHeatLayer.setPrior) histHeatLayer.setPrior(data.prior);
        histHeatLayer.setLatLngs(points);
        if (data.error) { statusDiv.textContent = data.error; statusDiv.style.color = '#b3261e'; }
        else if (points.length === 0) { statusDiv.textContent = 'No data in this window yet'; statusDiv.style.color = '#b3261e'; }
        else { statusDiv.textContent = `${points.length.toLocaleString('en-AU')} cells loaded`; statusDiv.style.color = ''; }
      } catch (e) {
        if (seq !== histRequestSeq) return;
        statusDiv.textContent = 'Fetch failed: ' + e; statusDiv.style.color = '#b3261e';
      } finally {
        if (seq === histRequestSeq) heatLoadingBanner.hidden = true;
      }
    }
    document.getElementById('histWindowSelect').addEventListener('change', loadHistoricalHeatmap);
    document.getElementById('histPeriodSelect').addEventListener('change', loadHistoricalHeatmap);

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
      if (metric === 'frequency') {
        // Live is necessarily instantaneous (one poll = one reading per
        // vehicle, not a rolling rate), so this is just "is this bus off
        // the KPI window right now": 1 if early/late, 0 if on time.
        if (v.delay_min == null || v.anomaly || v.on_time == null) return null;
        return v.on_time ? 0 : 1;
      }
      // delay (default): weight by this vehicle's own lateness, not by
      // 1-per-vehicle — otherwise the heat layer just draws the route
      // network (wherever buses happen to be) rather than where they're
      // currently running late. Vehicles with no delay reading or a
      // flagged anomaly don't contribute a heat point (still shown as a
      // marker), since we can't say whether they're a bottleneck or not.
      if (v.delay_min == null || v.anomaly) return null;
      const cap = metric === 'delay' ? DELAY_CAP_MIN : SEVERITY_CAP_MIN;
      return Math.min(Math.max(v.delay_min, 0) / cap, 1);
    }

    function updateLiveHeatFromVehicles(vehicles) {
      const heatPoints = [];
      vehicles.forEach(v => {
        if (v.lat == null || v.lon == null) return;
        const w = liveWeightFor(v, currentMetric);
        if (w !== null) heatPoints.push([v.lat, v.lon, w]);
      });
      liveHeatData = heatPoints;
      heatLayer.setLatLngs(heatPoints);
    }

    function applyMetric(metric) {
      currentMetric = metric;
      const cfg = METRICS[metric];
      heatLayer = swapHeatLayer(heatLayer, metric, 'live', LIVE_LAYER_NAME, liveHeatData);
      histHeatLayer = swapHeatLayer(histHeatLayer, metric, 'hist', HIST_LAYER_NAME, histHeatData);
      fillLegend(liveLegend, cfg.liveGradient, cfg.liveTitle, cfg.ticks);
      fillLegend(histLegend, cfg.histGradient, cfg.histTitle, cfg.ticks);
      heatPanel.querySelector('#heatMetricDesc').textContent = cfg.desc || '';
      updateLiveHeatFromVehicles(lastVehicles);
      loadHistoricalHeatmap();
    }
    document.getElementById('heatMetricSelect').addEventListener('change', e => applyMetric(e.target.value));

    applyMetric(DEFAULT_METRIC);
    loadRouteShapes();

    // The on-time outline colour (OUTLINE_ON_TIME, server-side) is white —
    // fine as a thin ring around a solid blue pill, but used as an entire
    // glyph's fill on its own it vanishes against a light basemap. Late
    // (red) and early (green) should still read as red/green on the arrow,
    // same as the pill's border; only on-time swaps to the pill's own blue
    // so it stays visible without losing the red/green severity meaning.
    const ON_TIME_OUTLINE_HEX = '#ffffff';
    function makeIcon(routeLabel, bearing, outlineColor) {
      const rot = (bearing != null ? bearing : 0) - 90;
      // 'arrow' style: same bearing rotation as the pill's own arrow, just
      // without the label/background chrome — for a lighter-weight view
      // when the map is busy with vehicles.
      if (markerStyle === 'arrow') {
        const arrowColor = (outlineColor || '').toLowerCase() === ON_TIME_OUTLINE_HEX ? 'var(--fill-blue)' : outlineColor;
        return L.divIcon({ className:'', iconSize:[56,24], iconAnchor:[28,12], popupAnchor:[0,-12],
          html:`<div class="bus-marker"><div class="bus-arrow-only"><span class="arrow-glyph" style="color:${arrowColor}; transform:rotate(${rot}deg);">&#10148;</span></div></div>` });
      }
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
"""


def render_map_script(map_init_js):
    """Prefix HEATMAP_SCRIPT with the one page-specific line that creates
    the Leaflet map object (everything after it is identical for both
    pages)."""
    return map_init_js + "\n" + HEATMAP_SCRIPT


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
  th.help { text-decoration:underline dotted; text-underline-offset:2px; cursor:help; }
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
  .bus-arrow-only { position:absolute; top:0; left:50%; transform:translateX(-50%); width:24px; height:24px; display:flex; align-items:center; justify-content:center; }
  .bus-arrow-only .arrow-glyph { display:inline-block; font-size:20px; line-height:1; text-shadow:0 0 2px #fff, 0 0 4px #fff, 0 1px 2px rgba(0,0,0,0.35); }
  .leaflet-popup-content { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif; }
  .glass-tooltip { background:rgba(255,255,255,0.55) !important; -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45) !important; border-radius:12px !important; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:7px 11px; }
  .glass-tooltip::before { display:none; }
  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper { background:rgba(255,255,255,0.55); -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45); border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; }
  .leaflet-popup.glass-popup .leaflet-popup-tip { background:rgba(255,255,255,0.55); box-shadow:none; }
  .leaflet-control-layers { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif !important; }
  .heat-panel { font:12px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#333; margin-top:6px; padding-top:6px; border-top:1px solid #ddd; width:230px; }
  .heat-panel .hp-section { font-size:10.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; color:#777; margin:8px 0 4px; }
  .heat-panel .hp-section:first-child { margin-top:0; }
  .heat-panel .hp-grid { display:grid; grid-template-columns:auto 1fr; gap:4px 8px; align-items:center; }
  .heat-panel .hp-grid label { color:#555; }
  .heat-panel select { font:inherit; width:100%; min-width:0; }
  .heat-panel .hp-inline { display:flex; align-items:center; gap:4px; min-width:0; }
  .heat-panel #routeMaskStatus { color:#888; white-space:nowrap; }
  .heat-panel #histWindowStatus { color:#777; font-size:11px; margin-top:4px; }
  .heat-panel .heat-legend { margin:8px 0 0; }
  .heat-panel .hp-desc { color:#666; font-size:11px; line-height:1.35; margin-top:6px; }
  .heat-help { margin:10px 0 0; font-size:0.85rem; color:#444; }
  .heat-help summary { cursor:pointer; font-weight:600; }
  .heat-help p { margin:8px 0; }
  .heat-help dl { margin:6px 0 0; display:grid; grid-template-columns:max-content 1fr; gap:4px 14px; }
  .heat-help dt { font-weight:600; }
  .heat-help dd { margin:0; }
  #histWindowStatus { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#b3261e; margin:2px 0 2px 22px; max-width:220px; }
  .heat-legend { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; margin:6px 22px 2px; color:#333; }
  .heat-legend .heat-legend-title { font-weight:600; margin-bottom:2px; }
  .heat-legend .heat-legend-bar { height:10px; border-radius:2px; border:1px solid rgba(0,0,0,0.15); }
  .heat-legend .heat-legend-ticks { display:flex; justify-content:space-between; color:#888; margin-top:1px; }
  #heatLoadingBanner { position:absolute; top:10px; left:50%; transform:translateX(-50%); z-index:900; background:rgba(17,17,17,0.85); color:#fff; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; box-shadow:0 2px 8px rgba(0,0,0,0.25); pointer-events:none; }
</style>
</head>
<body>
  <h1>Delay Board <a class="toggle" style="float:right; font-size:0.8rem; font-weight:normal; border-bottom:none;" href="/project">Projector mode &rarr;</a></h1>
  <div class="meta">
    Data fetched {{ pulled_at }}
    {% if agency_error %}<br><span style="color:#b3261e">Operator names unavailable: {{ agency_error }}</span>{% endif %}
  </div>

  <form method="get" style="margin:20px 0; padding:14px; border:1px solid var(--line);">
    <input type="hidden" name="hide_anomalies" value="{{ 1 if hide_anomalies else 0 }}">
    <label>Operator <select name="operator" style="font-family:inherit;">
      <option value="">All operators</option>
      {% for op in operator_options %}<option value="{{ op }}"{% if op|lower == q_operator %} selected{% endif %}>{{ op }}</option>{% endfor %}
    </select></label>
    &nbsp;&nbsp;
    <label>Route <input type="text" name="route" value="{{ q_route }}" autocomplete="off" style="font-family:inherit;"></label>
    &nbsp;&nbsp;
    <label>Stop ID <input type="text" name="stop" value="{{ q_stop }}" autocomplete="off" style="font-family:inherit;"></label>
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
    <span>{{ vehicles|length }} buses{% if filters_active %} (filtered){% endif %} {% if apply_bounds %}within 10 km of the CBD &middot; <a class="toggle" href="?bounds=0&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">show Opal area</a>{% else %}across the Opal area &middot; <a class="toggle" href="?bounds=1&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">show 10 km of the CBD only</a>{% endif %}</span>
  </div>
  {% if map_error %}<div class="map-error">Vehicle positions unavailable: {{ map_error }}</div>{% endif %}

  <details class="heat-help">
    <summary>About the heatmaps</summary>
    <p><strong>Live heatmap</strong> colours each bus by its current value. <strong>Historical heatmap</strong> uses every scraped reading in the chosen history window (default 30 days), optionally limited to a time of day. <strong>Route clip</strong> trims colour to within the chosen distance of a bus route, so results read along corridors.</p>
    <dl>
      <dt>Average delay</dt><dd>How late buses typically run in each area: mean lateness of readings within 250 m, with early running counted as on time (0). Where data is thin, the estimate is pulled toward the network average, so a single late bus can&rsquo;t create a hotspot. Scale 0&ndash;5 min.</dd>
      <dt>Delay burden</dt><dd>Where the most delay accumulates: total bus-minutes late, scaled to the same overall heat as Bus density. Busy corridors rank high even when each bus is only slightly late. An area hotter here than on Bus density carries more than its share of the network&rsquo;s delay.</dd>
      <dt>Not on time</dt><dd>Share of readings within 250 m that fall outside the TfNSW on-time window (0:59 early to 5:59 late), so early running counts as well as late. Thin data is pulled toward the network average. Scale 0&ndash;60%.</dd>
      <dt>Speed</dt><dd>Average bus speed within 160 m, including time stopped at stops and signals. Amber = CBD streets, pink = arterials, purple = motorways. Scale 0&ndash;80 km/h.</dd>
      <dt>Bus density</dt><dd>How many bus position readings were recorded in each area: where buses run most, not how well they run.</dd>
    </dl>
  </details>

  <h2>By operator</h2>
  <table>
    <tr><th>Operator</th><th class="help" title="Distinct buses reporting, using each bus’s latest stop. Anomalous readings (over an hour early or late) are excluded.">n</th><th>avg delay</th><th class="help" title="Standard deviation (SD) of delay in minutes: how much delay varies between buses, not how late they run on average. Low = consistently delayed by about the same amount; high = some buses run much later (or earlier) than others.">spread (&plusmn;min)</th><th class="help" title="Share of buses between 0:59 early and 5:59 late (TfNSW on-time running KPI window).">on time</th><th>range</th></tr>
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
    <tr><th>Route</th><th>Operator</th><th class="help" title="Distinct buses reporting, using each bus’s latest stop. Anomalous readings (over an hour early or late) are excluded.">n</th><th>avg delay</th><th class="help" title="Standard deviation (SD) of delay in minutes: how much delay varies between buses, not how late they run on average. Low = consistently delayed by about the same amount; high = some buses run much later (or earlier) than others.">spread (&plusmn;min)</th><th class="help" title="Share of buses between 0:59 early and 5:59 late (TfNSW on-time running KPI window).">on time</th><th>range</th></tr>
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
""" + render_map_script(
"""    const map = L.map('dashmap').setView([-33.8688, 151.2093], 11);"""
) + """
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
<title>Delay Board — projector mode</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
<style>
  :root { --fill-blue: {{ color_fill }}; }
  * { box-sizing: border-box; }
  html, body { height:100%; }
  body { margin:0; background:#e5e3dc; }
  #dashmap { position:absolute; inset:0; }
  .bus-marker { position:relative; width:56px; height:24px; }
  .bus-pill { position:absolute; top:0; left:50%; transform:translateX(-50%); display:flex; align-items:center; gap:4px; background:var(--fill-blue); color:#fff; font:600 11px/1 -apple-system, Helvetica, Arial, sans-serif; padding:5px 7px; border-radius:7px; border:2.5px solid #888; box-shadow:0 1px 3px rgba(0,0,0,0.4); white-space:nowrap; }
  .bus-arrow { flex:0 0 auto; font-size:12px; line-height:1; display:inline-block; color:#fff; }
  .bus-arrow-only { position:absolute; top:0; left:50%; transform:translateX(-50%); width:24px; height:24px; display:flex; align-items:center; justify-content:center; }
  .bus-arrow-only .arrow-glyph { display:inline-block; font-size:20px; line-height:1; text-shadow:0 0 2px #fff, 0 0 4px #fff, 0 1px 2px rgba(0,0,0,0.35); }
  .leaflet-popup-content { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif; }
  .glass-tooltip { background:rgba(255,255,255,0.55) !important; -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45) !important; border-radius:12px !important; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:7px 11px; }
  .glass-tooltip::before { display:none; }
  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper { background:rgba(255,255,255,0.55); -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45); border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; }
  .leaflet-popup.glass-popup .leaflet-popup-tip { background:rgba(255,255,255,0.55); box-shadow:none; }
  .leaflet-control-layers { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif !important; }
  .heat-panel { font:12px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#333; margin-top:6px; padding-top:6px; border-top:1px solid #ddd; width:230px; }
  .heat-panel .hp-section { font-size:10.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; color:#777; margin:8px 0 4px; }
  .heat-panel .hp-section:first-child { margin-top:0; }
  .heat-panel .hp-grid { display:grid; grid-template-columns:auto 1fr; gap:4px 8px; align-items:center; }
  .heat-panel .hp-grid label { color:#555; }
  .heat-panel select { font:inherit; width:100%; min-width:0; }
  .heat-panel .hp-inline { display:flex; align-items:center; gap:4px; min-width:0; }
  .heat-panel #routeMaskStatus { color:#888; white-space:nowrap; }
  .heat-panel #histWindowStatus { color:#777; font-size:11px; margin-top:4px; }
  .heat-panel .heat-legend { margin:8px 0 0; }
  .heat-panel .hp-desc { color:#666; font-size:11px; line-height:1.35; margin-top:6px; }
  #histWindowStatus { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#b3261e; margin:2px 0 2px 22px; max-width:220px; }
  .heat-legend { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; margin:6px 22px 2px; color:#333; }
  .heat-legend .heat-legend-title { font-weight:600; margin-bottom:2px; }
  .heat-legend .heat-legend-bar { height:10px; border-radius:2px; border:1px solid rgba(0,0,0,0.15); }
  .heat-legend .heat-legend-ticks { display:flex; justify-content:space-between; color:#888; margin-top:1px; }
  #heatLoadingBanner { position:absolute; top:10px; left:50%; transform:translateX(-50%); z-index:900; background:rgba(17,17,17,0.85); color:#fff; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; box-shadow:0 2px 8px rgba(0,0,0,0.25); pointer-events:none; }
  .project-mask { position:absolute; background:#000; z-index:850; pointer-events:none; }
</style>
</head>
<body>
  <div id="dashmap"></div>
  <script>
    const map = L.map('dashmap', {
      // This view just loads the projector's area and sits there — no
      // panning, no zooming, in or out. Bus markers, both heat layers and
      // the metric/window pickers (from the shared script below) are the
      // only things on this page a viewer can ever interact with.
      zoomControl: false, dragging: false, touchZoom: false, doubleClickZoom: false,
      scrollWheelZoom: false, boxZoom: false, keyboard: false, tap: false,
      // Leaflet only zooms in whole-number steps by default, so fitBounds()
      // below would round DOWN to the nearest whole zoom that still fully
      // contains PROJECT_BOUNDS — leaving a big chunk of the container
      // unused (masked solid black) even when the browser/projector aspect
      // ratio matches the physical box closely. Fractional zoom lets it
      // lock to the exact zoom that fills the container, so the only
      // black margin left is genuine letterboxing from an aspect mismatch.
      zoomSnap: 0,
      zoomDelta: 0,
    });
    const PROJECT_BOUNDS = L.latLngBounds([{{ sw_lat }}, {{ sw_lng }}], [{{ ne_lat }}, {{ ne_lng }}]);
    map.fitBounds(PROJECT_BOUNDS);
    map.setMinZoom(map.getZoom());
    map.setMaxZoom(map.getZoom());
    map.setMaxBounds(PROJECT_BOUNDS);
""" + HEATMAP_SCRIPT + """

    // This view gets fed straight to a physical projector, so anything
    // the browser shows outside the calibrated real-world box must be
    // solid black, not map/tiles/ocean colour — otherwise the projector
    // paints stray imagery past the edge of the physical model. The map
    // itself is fully locked (no pan/zoom at all — see above), but on a
    // browser window whose aspect ratio doesn't exactly match the box's,
    // fitBounds still leaves a margin on one axis (letterboxing) — that's
    // what these four bars blank out. Recomputed on 'resize' (in case the
    // window/output resolution changes) and on the initial move/zoom
    // fitBounds itself fires, so it's exact regardless of window size.
    const maskTop = document.createElement('div');
    const maskBottom = document.createElement('div');
    const maskLeft = document.createElement('div');
    const maskRight = document.createElement('div');
    [maskTop, maskBottom, maskLeft, maskRight].forEach(el => {
      el.className = 'project-mask';
      document.body.appendChild(el);
    });
    function updateProjectMask() {
      const size = map.getSize();
      const nw = map.latLngToContainerPoint(PROJECT_BOUNDS.getNorthWest());
      const se = map.latLngToContainerPoint(PROJECT_BOUNDS.getSouthEast());
      const left = Math.max(0, Math.min(nw.x, size.x));
      const top = Math.max(0, Math.min(nw.y, size.y));
      const right = Math.max(0, Math.min(se.x, size.x));
      const bottom = Math.max(0, Math.min(se.y, size.y));
      maskTop.style.cssText    = `left:0; top:0; width:100%; height:${top}px;`;
      maskBottom.style.cssText = `left:0; top:${bottom}px; width:100%; height:${Math.max(0, size.y - bottom)}px;`;
      maskLeft.style.cssText   = `left:0; top:${top}px; width:${left}px; height:${Math.max(0, bottom - top)}px;`;
      maskRight.style.cssText  = `left:${right}px; top:${top}px; width:${Math.max(0, size.x - right)}px; height:${Math.max(0, bottom - top)}px;`;
    }
    map.on('move zoom resize', updateProjectMask);
    updateProjectMask();
    window.addEventListener('resize', () => { map.invalidateSize(); updateProjectMask(); });
  </script>
</body>
</html>
"""


def _fmt_pulled(iso):
    """'2026-10-09T10:44:12.123456+11:00' -> '10:44:12 · Fri 9 Oct' (Sydney, 24 h)."""
    try:
        t = datetime.fromisoformat(iso).astimezone(SYDNEY_TZ)
    except (TypeError, ValueError):
        return iso
    return f"{t:%H:%M:%S} \u00b7 {t:%a} {t.day} {t:%b}"


@app.route("/project")
def project():
    if not API_KEY:
        return "TFNSW_API_KEY not set in .env", 500
    data = compute_delay_data(request.args)
    vehicles, map_error = compute_vehicles(data)
    ne_lat, ne_lng = SMART_CITY_CORNER_NE
    sw_lat, sw_lng = SMART_CITY_CORNER_SW
    return render_template_string(
        PROJECT_PAGE,
        vehicles=vehicles, vehicles_json=json.dumps(vehicles),
        color_fill=COLOR_FILL,
        ne_lat=ne_lat, ne_lng=ne_lng, sw_lat=sw_lat, sw_lng=sw_lng,
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
            "cells": len(entry["cells"].get("all", {})),
            "cells_by_period": {p: len(c) for p, c in entry["cells"].items()},
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
        pulled_at=_fmt_pulled(all_rows[0]["pulled_at"]) if all_rows else "-",
        n_total=len(all_rows),
        n_flagged=sum(1 for r in all_rows if r["anomaly"]),
        hide_anomalies=data["hide_anomalies"],
        q_route=data["q_route"], q_stop=data["q_stop"], q_operator=data["q_operator"],
        agency_error=data["agency_error"],
        operators=operators, routes=routes, worst_trips=worst_trips,
        vehicles=vehicles, vehicles_json=json.dumps(vehicles),
        filters_active=data["filters_active"], apply_bounds=data["apply_bounds"],
        operator_options=data["operator_options"],
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


def get_route_shapes_gz():
    """Gzipped route_shapes.json bytes, or None if not built/fetchable yet.
    Failures aren't cached, so the mask turns on as soon as the file exists."""
    now = time.monotonic()
    if _route_shapes_cache["gz"] and now - _route_shapes_cache["fetched_at"] < ROUTE_SHAPES_CACHE_TTL_SECONDS:
        return _route_shapes_cache["gz"]
    with _route_shapes_lock:
        now = time.monotonic()
        if _route_shapes_cache["gz"] and now - _route_shapes_cache["fetched_at"] < ROUTE_SHAPES_CACHE_TTL_SECONDS:
            return _route_shapes_cache["gz"]
        try:
            r = requests.get(ROUTE_SHAPES_URL, timeout=30)
            if r.status_code != 200:
                print(f"[shapes] GET {ROUTE_SHAPES_URL} -> HTTP {r.status_code}", flush=True)
                return _route_shapes_cache["gz"]  # keep serving a stale copy if we have one
            json.loads(r.content)  # don't cache a broken file
        except (requests.RequestException, ValueError) as e:
            print(f"[shapes] fetch failed: {e}", flush=True)
            return _route_shapes_cache["gz"]
        _route_shapes_cache.update(gz=gzip.compress(r.content, 6), fetched_at=now)
        print(f"[shapes] cached {len(r.content) / 1e3:.0f} kB "
              f"({len(_route_shapes_cache['gz']) / 1e3:.0f} kB gzipped)", flush=True)
        return _route_shapes_cache["gz"]


@app.route("/api/route_shapes")
def api_route_shapes():
    gz = get_route_shapes_gz()
    if gz is None:
        return jsonify({"lines": [], "error": "Route shapes not built yet"})
    if "gzip" in request.headers.get("Accept-Encoding", ""):
        resp = Response(gz, mimetype="application/json")
        resp.headers["Content-Encoding"] = "gzip"
        resp.headers["Vary"] = "Accept-Encoding"
    else:
        resp = Response(gzip.decompress(gz), mimetype="application/json")
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/api/heatmap")
def api_heatmap():
    try:
        try:
            window_hours = int(request.args.get("window", HEATMAP_DEFAULT_WINDOW))
        except ValueError:
            window_hours = 24
        if window_hours not in HEATMAP_WINDOWS:
            window_hours = HEATMAP_DEFAULT_WINDOW
        metric = request.args.get("metric", "delay")
        if metric not in VALID_HEATMAP_METRICS:
            metric = "delay"
        period = request.args.get("period", "all")
        if period not in VALID_HEATMAP_PERIODS:
            period = "all"
        points, error, prior = get_heatmap_points_cached(window_hours, metric, period)
        return jsonify({"points": points, "prior": prior, "window_hours": window_hours,
                        "metric": metric, "period": period, "error": error})
    except Exception as e:
        return jsonify({"points": [], "window_hours": None, "metric": None, "period": None,
                        "error": f"Server error: {e}"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)