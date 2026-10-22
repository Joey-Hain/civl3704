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
"delay_total", "frequency", "density", or "speed", and a
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
import struct
import os
import shutil
import statistics
import tempfile
import threading
import time
import zipfile
import zlib
from array import array
from collections import OrderedDict, defaultdict
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
# downloads hide GitHub's per-request latency for the 7-day window
# (~31 files; ~13 MB/day since the collector went 24/7, cached on disk)
# without each worker holding its own copy of the aggregated cells, which is what kept the old parse-in-workers design at 1.
HEATMAP_DOWNLOAD_CONCURRENCY = 6
HEATMAP_DEADLINE_SEC = 20
# Historical windows offered (hours). A 30-day window (720) was added when
# the scrape only ran 3-6 times a day and 7 days averaged ~1.3 readings per
# 10 m cell. Since 12 Oct the collector snapshots every 5 minutes around the
# clock (~130k rows a day), so 7 days now holds more readings than 30 days
# used to, while 30 days (~4M rows) is too much to parse on Render's free
# CPU. The longest window, and custom ranges, are therefore capped at 7 days.
HEATMAP_WINDOWS = (1, 24, 168)
HEATMAP_DEFAULT_WINDOW = 168
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
VALID_HEATMAP_METRICS = ("delay", "delay_mean", "delay_total",
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

# MEMORY (16 Oct): Render killed the instance for using over 512 MB about
# three minutes after every restart, although the RSS logged at each
# milestone stayed near 100 MB. The live trip-update feed is rebuilt every
# 12 s into tens of thousands of dicts, from up to six gunicorn threads, and
# glibc gives each thread its own malloc arena that keeps freed memory, so
# the process grew with every rebuild. Two arenas, plus malloc_trim() after
# each big rebuild, hand that memory back. Linux/glibc only; a no-op elsewhere.
try:
    import ctypes
    _libc = ctypes.CDLL("libc.so.6")
    _libc.mallopt(-8, 2)  # M_ARENA_MAX = 2
except (OSError, AttributeError):
    _libc = None


def _trim_memory():
    if _libc is not None:
        try:
            _libc.malloc_trim(0)
        except Exception:
            pass
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
# Per-trip lookup behind "click a bus to see its route and upcoming stops"
# (/api/trip). Built by the same weekly gtfs-r-scrape job as the route mask:
# trip_id -> shape_id and shape geometry, each split into n_shards small
# files by zlib.crc32(id) % n_shards, plus stop names/coordinates. Only the
# shards a click needs are fetched, and a bounded LRU keeps the busiest
# ones, so the full timetable never has to sit in this 512 MB instance.
# Upcoming stops and their predicted delays come from the live trip-update
# feed already cached by get_all_rows_cached().
TRIP_LOOKUP_BASE = f"https://raw.githubusercontent.com/{SCRAPE_REPO}/main/shapes"
TRIP_LOOKUP_TTL_SECONDS = 6 * 3600
TRIP_SHARD_CACHE_MAX = 64

# /project page: a Leaflet view locked to the exact real-world box
# TransportLab's smart-city rig projects onto its physical table model, so
# it can be checked against the physical model for alignment. Hardcoded
# from that project's params.json5 (model.corners.ne / model.corners.sw:
# https://github.com/TransportLab/smart-city/blob/main/params.json5) rather
# than fetched live; they only change if the physical model is rebuilt or
# the rig is re-tuned.
#SMART_CITY_CORNER_NE = (-33.833633, 151.264819)
#SMART_CITY_CORNER_SW = (-33.896399, 151.142800)
SMART_CITY_CORNER_NE = (-33.835, 151.265)
SMART_CITY_CORNER_SW = (-33.895, 151.145)
# Projector geometry from the same file. smart-city lays the map out as a
# full-width box with the model's aspect ratio (model.width / model.height),
# vertically centred (flexbox, so margin-top: vertical_offset shifts it by
# half that), then stretches it horizontally with transform: scaleX(
# horizontal_scale) about its centre — values hand-tuned on the real rig.
# /project reproduces that layout exactly as its default (uncalibrated)
# projection, so the model's corners land where the rig expects them.
SMART_CITY_MODEL_MM = (2400, 1200)            # model.width, model.height
SMART_CITY_PROJ_VERTICAL_OFFSET_PX = 50       # projector.vertical_offset
SMART_CITY_PROJ_HORIZONTAL_SCALE = 1.1839999999999904  # projector.horizontal_scale

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


def _rss_mb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return None


_last_rss_log = [0.0]


def _log_rss_every(tag, period_sec=60):
    """Log RSS at most once a minute, so a slow climb shows up in Render's
    logs before an out-of-memory kill rather than only at milestones."""
    now = time.monotonic()
    if now - _last_rss_log[0] >= period_sec:
        _last_rss_log[0] = now
        _log_rss(tag)


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

# Values the shared map script (HEATMAP_SCRIPT, used by both "/" and
# "/project") needs on every page, so each page's render call doesn't
# have to remember to pass them.
app.jinja_env.globals.update(
    on_time_early_sec=ON_TIME_EARLY_SEC, on_time_late_sec=ON_TIME_LATE_SEC,
    status_late=OUTLINE_LATE, status_early=OUTLINE_EARLY, status_on_time=COLOR_FILL,
)

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
# /api/trip lookups: manifest + stops (refreshed together) and an LRU of
# fetched shards keyed (kind, shard), kind = "trips" or "geom".
_trip_meta_cache = {"n_shards": None, "generated": None, "stops": None, "fetched_at": None}
_trip_shards = OrderedDict()
_trip_lookup_lock = threading.Lock()


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
        del feed
        # (The CSV append of every row to CIVL3704/delay_log.csv that used
        # to happen here was removed: nothing read it, Render's disk is
        # wiped on every restart, and it wrote MBs every 12 s.)

        _rows_cache.update(all_rows=all_rows, agency_names=agency_names, trip_headsigns=trip_headsigns,
                            agency_error=agency_error, fetched_at=now)
        _trim_memory()
        _log_rss_every(f"live feed rows={len(all_rows)}")
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


# --- Historical cell storage -------------------------------------------------
# Column layout of one grid cell's accumulator (see CellGrid):
#   LAT_SUM/LON_SUM/N_TOTAL  - plotted position (mean vehicle location) and ping
#                              count; N_TOTAL alone is what density weights by.
#   LATE_SUM/N_DELAY         - mean lateness: seconds late floored at 0 (early or
#                              on-time readings add 0, never a negative that would
#                              mask a late reading elsewhere in the cell).
#   SPEED_SUM/N_SPEED        - mean speed; a 0 km/h reading is kept (a real stop),
#                              a missing one is not.
#   N_OFF                    - readings outside the TfNSW on-time window, for the
#                              "not on time" (frequency) metric.
#
# MEMORY: until 16 Oct each cell kept every raw delay reading (an array) so the
# frequency metric could count off-time readings later, and cells lived in a
# dict of Python lists. That was sized for ~200 kB/day of scrape data. Since the
# collector went 24/7 (12 Oct) a day is ~13 MB / ~130k rows, and building the
# 24 h + 7 day + 30 day windows peaked at ~570 MB, over Render's 512 MB, so the
# instance was OOM-killed and restarted (the 503s). Off-time readings are now
# counted as they arrive, and a finished grid is frozen into 8 flat double
# arrays (64 bytes per cell), so memory scales with the number of cells, which
# levels off as the network fills in, not with the number of readings.
LAT_SUM, LON_SUM, N_TOTAL, LATE_SUM, N_DELAY, SPEED_SUM, N_SPEED, N_OFF = range(8)
_CELL_COLS = 8


class CellGrid:
    """Grid-cell accumulators for one period bucket, as 8 parallel array('d')
    columns. While building, `index` maps a packed integer cell key to its
    row; freeze() drops that map once the window is complete, because nothing
    after aggregation needs to look a cell up by position."""

    __slots__ = ("index", "cols")

    def __init__(self):
        self.index = {}
        self.cols = [array("d") for _ in range(_CELL_COLS)]

    def __len__(self):
        return len(self.cols[0])

    def add(self, key, lat, lon, delay_sec, speed_kmh, off_time):
        i = self.index.get(key)
        cols = self.cols
        if i is None:
            i = len(cols[0])
            self.index[key] = i
            for col in cols:
                col.append(0.0)
        cols[LAT_SUM][i] += lat
        cols[LON_SUM][i] += lon
        cols[N_TOTAL][i] += 1
        if delay_sec is not None:
            cols[LATE_SUM][i] += delay_sec if delay_sec > 0 else 0.0
            cols[N_DELAY][i] += 1
            if off_time:
                cols[N_OFF][i] += 1
        if speed_kmh is not None and speed_kmh >= 0:
            cols[SPEED_SUM][i] += speed_kmh
            cols[N_SPEED][i] += 1

    def add_cells(self, keys, cols):
        """Sum another grid's cells (keys + 8 columns) into this one."""
        index, mine = self.index, self.cols
        for j, key in enumerate(keys):
            i = index.get(key)
            if i is None:
                index[key] = len(mine[0])
                for c, src in zip(mine, cols):
                    c.append(src[j])
            else:
                for c, src in zip(mine, cols):
                    c[i] += src[j]

    def freeze(self):
        self.index = None
        return self

    def rows(self):
        """One tuple per cell, indexed by the column constants above."""
        return zip(*self.cols)


def _new_period_cells():
    return {p: CellGrid() for p in VALID_HEATMAP_PERIODS}


_GRID_SCALE = 10 ** GRID_DECIMALS


def _cell_key(lat, lon):
    # One int instead of a (lat, lon) float tuple: a fraction of the memory
    # per dict entry while the grid is being built. |lon| * 10^4 < 10^7.
    return round(lat * _GRID_SCALE) * 10_000_000 + round(lon * _GRID_SCALE)


# --- Fetching the daily CSVs --------------------------------------------------
# REQUESTS / 429s: every rebuild used to re-download every day in the window
# from both data/ and data-local/ (62 requests for 30 days, ~60 MB now), every
# 5-30 minutes, from Render's shared outbound IPs, and raw.githubusercontent.com
# started answering 429 Too Many Requests. Days before yesterday never change
# once the collector has moved on, so each is downloaded once per instance and
# kept gzipped on local disk; only today and yesterday are re-fetched. A 404
# (no data-local file for that day, the usual case) is remembered for a few
# hours instead of being asked again each rebuild. If a re-fetch of today or
# yesterday fails, the copy already on disk is used. Set GITHUB_TOKEN (a
# fine-grained token with no permissions is enough) to send authenticated
# requests, which GitHub rate-limits per token rather than per shared IP.
HEATMAP_DISK_CACHE_DIR = Path(os.getenv(
    "HEATMAP_DISK_CACHE_DIR", os.path.join(tempfile.gettempdir(), "civl3704_days")))
HEATMAP_MUTABLE_DAYS = 1           # today and yesterday are always re-fetched
MISSING_FILE_RECHECK_SEC = 6 * 3600         # older days
MISSING_RECENT_FILE_RECHECK_SEC = 30 * 60   # today/yesterday
RETRYABLE_STATUS = (429, 500, 502, 503, 504)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
RAW_HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
_missing_files = {}  # cache path -> time.monotonic() the 404 was seen


def _day_cache_path(base, date_str):
    return HEATMAP_DISK_CACHE_DIR / f"{base.rsplit('/', 1)[-1]}_{date_str}.csv.gz"


def _retry_after_sec(resp, default=2.0, cap=5.0):
    try:
        return min(float(resp.headers.get("Retry-After", default)), cap)
    except (TypeError, ValueError):
        return default


def _download_day(date_str, base, mutable):
    """(date_str, local gzip path or None, error or None). A None path with
    no error means the file doesn't exist. Network + disk only, so it is safe
    to run in a thread pool."""
    path = _day_cache_path(base, date_str)
    if not mutable and path.exists():
        return date_str, path, None
    seen = _missing_files.get(path)
    recheck = MISSING_RECENT_FILE_RECHECK_SEC if mutable else MISSING_FILE_RECHECK_SEC
    if seen is not None and time.monotonic() - seen < recheck:
        return date_str, None, None
    url = f"{base}/{date_str}.csv"
    error = None
    for attempt in range(2):
        try:
            with requests.get(url, headers=RAW_HEADERS, timeout=30, stream=True) as r:
                print(f"[heatmap] GET {url} -> HTTP {r.status_code}", flush=True)
                if r.status_code == 404:
                    _missing_files[path] = time.monotonic()
                    return date_str, None, None
                if r.status_code in RETRYABLE_STATUS and attempt == 0:
                    time.sleep(_retry_after_sec(r))
                    continue
                r.raise_for_status()
                HEATMAP_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                tmp = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")
                with gzip.open(tmp, "wb", compresslevel=1) as f:
                    for chunk in r.iter_content(256 * 1024):
                        f.write(chunk)
                os.replace(tmp, path)
                _missing_files.pop(path, None)
                return date_str, path, None
        except (requests.RequestException, OSError) as e:
            error = f"{date_str}: {e}"
            if attempt == 0:
                time.sleep(1)
    if path.exists():
        # Today's/yesterday's refresh failed: an older copy beats a hole.
        return date_str, path, f"{error} (used the copy from an earlier fetch)"
    if path in _missing_files:
        return date_str, None, None  # was a 404 last time; still assume missing
    return date_str, None, error


def _prune_disk_cache(keep_days=None):
    if keep_days is None:
        keep_days = CUSTOM_WINDOW_LOOKBACK_DAYS + 2
    cutoff = (datetime.now(tz=SYDNEY_TZ).date() - timedelta(days=keep_days)).isoformat()
    try:
        for p in HEATMAP_DISK_CACHE_DIR.glob("*.csv.gz"):
            if p.name.split("_", 1)[-1][:10] < cutoff:
                p.unlink(missing_ok=True)
    except OSError:
        pass


def _fetch_one_day_into(date_str, path, cutoff, cells_by_period, end=None, agency_stats=None):
    """Stream one day's (gzipped, on-disk) CSV into cells_by_period: every
    row lands in its "all" grid and its time-of-day grid
    (time_of_day_bucket(ts)) in a single pass. Returns
    (date_str, rows_seen, points_added, error)."""
    rows_seen = 0
    points_added = 0
    grid_all = cells_by_period["all"]
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                header = [h.strip() for h in next(reader)]
            except StopIteration:
                return date_str, 0, 0, None
            try:
                ts_idx = header.index("timestamp")
                lat_idx = header.index("lat")
                lon_idx = header.index("lon")
            except ValueError:
                return date_str, 0, 0, f"{date_str}: missing required columns"
            delay_idx = header.index("delay_sec") if "delay_sec" in header else None
            speed_idx = header.index("speed_kmh") if "speed_kmh" in header else None
            route_idx = header.index("route_id") if "route_id" in header else None
            max_idx = max(ts_idx, lat_idx, lon_idx)
            for i, row in enumerate(reader):
                if i % 2000 == 0:
                    time.sleep(PARSE_YIELD_SEC)  # let request threads in
                rows_seen += 1
                if len(row) <= max_idx:
                    continue
                ts = _parse_ts_cached(row[ts_idx])
                if ts is None or ts < cutoff or (end is not None and ts >= end):
                    continue
                try:
                    lat = float(row[lat_idx])
                    lon = float(row[lon_idx])
                except ValueError:
                    continue

                delay_sec = None
                if delay_idx is not None and len(row) > delay_idx and row[delay_idx]:
                    try:
                        delay_sec = float(row[delay_idx])
                    except ValueError:
                        delay_sec = None
                speed_kmh = None
                if speed_idx is not None and len(row) > speed_idx and row[speed_idx]:
                    try:
                        speed_kmh = float(row[speed_idx])
                    except ValueError:
                        speed_kmh = None
                off_time = delay_sec is not None and not is_on_time(delay_sec)

                if agency_stats is not None and delay_sec is not None and route_idx is not None \
                        and len(row) > route_idx and "_" in row[route_idx]:
                    # Per-operator on-time tallies for /api/validation, keyed by
                    # the agency id prefix of route_id ("2436_M30" -> "2436").
                    st = agency_stats[row[route_idx].split("_", 1)[0]]
                    st[0] += 1
                    if delay_sec < ON_TIME_EARLY_SEC:
                        st[1] += 1
                    elif delay_sec > ON_TIME_LATE_SEC:
                        st[3] += 1
                    else:
                        st[2] += 1
                    st[4] += max(delay_sec, 0.0)

                key = _cell_key(lat, lon)
                grid_all.add(key, lat, lon, delay_sec, speed_kmh, off_time)
                cells_by_period[time_of_day_bucket(ts)].add(key, lat, lon, delay_sec, speed_kmh, off_time)
                points_added += 1
            return date_str, rows_seen, points_added, None
    except (OSError, EOFError, csv.Error, UnicodeDecodeError) as e:
        try:
            path.unlink(missing_ok=True)  # corrupt cache file: re-fetch next time
        except OSError:
            pass
        return date_str, rows_seen, points_added, f"{date_str}: {e}"


def fetch_historical_cells(window_hours=None, start_dt=None, end_dt=None):
    """Fetch + aggregate a window's CSVs into grid cells. This is the
    expensive, metric-independent half of building a historical heatmap —
    see compute_metric_points() for turning these cells into a specific
    metric's weighted points.

    Either window_hours (the last N hours) or an explicit [start_dt, end_dt)
    range (custom dates). Also tallies on-time counts per operator agency id
    in the same pass, for /api/validation."""
    start = time.monotonic()
    now = datetime.now(tz=SYDNEY_TZ)
    if start_dt is None:
        cutoff, end_dt = now - timedelta(hours=window_hours), None
    else:
        cutoff = start_dt
    last_day = min((end_dt - timedelta(microseconds=1)).date() if end_dt else now.date(), now.date())
    first_mutable = now.date() - timedelta(days=HEATMAP_MUTABLE_DAYS)
    dates_needed = []
    d = cutoff.date()
    while d <= last_day:
        dates_needed.append(d)
        d += timedelta(days=1)
    # agency id -> [n_with_delay, n_early, n_on_time, n_late, late_sum_sec]
    agency_stats = defaultdict(lambda: [0, 0, 0, 0, 0.0])
    _prune_disk_cache()

    # Downloads (or disk-cache hits) run in parallel; parsing happens here,
    # one file at a time, into a single set of period grids.
    cells_by_period = _new_period_cells()
    files_fetched = 0
    rows_seen_total = 0
    points_added_total = 0
    errors = []
    files_lost = 0  # failed to download and no copy on disk
    deadline_hit = False

    workers = max(1, min(HEATMAP_DOWNLOAD_CONCURRENCY, 2 * len(dates_needed)))
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = {pool.submit(_download_day, d.isoformat(), base, d >= first_mutable)
                   for d in dates_needed for base in SCRAPE_RAW_BASES}
        for future in as_completed(futures):
            if time.monotonic() - start > HEATMAP_DEADLINE_SEC:
                deadline_hit = True
                break
            date_str, path, error = future.result()
            if error is not None:
                errors.append(error)
            if path is None:
                files_lost += error is not None
                continue
            date_str, rows_seen, points_added, error = _fetch_one_day_into(
                date_str, path, cutoff, cells_by_period, end=end_dt, agency_stats=agency_stats)
            if error is not None:
                errors.append(error)
            if rows_seen == 0 and points_added == 0:
                continue
            files_fetched += 1
            rows_seen_total += rows_seen
            points_added_total += points_added
    finally:
        # Don't block the request on downloads still in flight after a
        # deadline hit; they finish (or time out) in the background and land
        # in the disk cache for the next rebuild.
        pool.shutdown(wait=False, cancel_futures=True)

    for grid in cells_by_period.values():
        grid.freeze()
    cells = cells_by_period["all"]
    cells_with_delay = sum(1 for n in cells.cols[N_DELAY] if n > 0)
    cells_with_speed = sum(1 for n in cells.cols[N_SPEED] if n > 0)
    print(f"[heatmap] window={window_hours}h files={files_fetched} "
          f"rows={rows_seen_total} points={points_added_total} cells={len(cells)} "
          f"cells_with_delay={cells_with_delay} cells_with_speed={cells_with_speed} "
          f"by_period=({', '.join(f'{p}={len(cells_by_period[p])}' for p in TIME_PERIODS)}) "
          f"errors={len(errors)} lost={files_lost} elapsed={time.monotonic() - start:.1f}s "
          f"deadline_hit={deadline_hit}", flush=True)
    for e in errors[:5]:
        print(f"[heatmap]   {e}", flush=True)
    _log_rss(f"heatmap window={window_hours}h")

    return {
        "cells": cells_by_period,
        "agency_stats": dict(agency_stats),
        "files_fetched": files_fetched,
        "rows_seen_total": rows_seen_total,
        "last_error": errors[-1] if errors else None,
        "n_errors": len(errors),
        "files_lost": files_lost,
        "deadline_hit": deadline_hit,
    }


# --- Pre-aggregated cells (the normal path) ------------------------------------
# PARSING ON RENDER WAS THE BOTTLENECK: even with the memory fixes, a 7-day
# window is ~640k CSV rows, and Render's Free instance (a fraction of one CPU)
# couldn't parse them inside HEATMAP_DEADLINE_SEC, so the map came back
# partial and the instance stayed busy enough to fail health checks. The
# gtfs-r-scrape Action now does the aggregation after every data push
# (build_cells.py, ~8 s on GitHub's runners) and force-pushes the result to
# its `cells` branch: windows/{1,24,168}.bin.gz for the preset windows (ending
# at the newest snapshot) and days/YYYY-MM-DD.bin.gz per Sydney day for custom
# ranges. Here a preset window is one download (conditional, so usually a 304)
# and a few array.frombytes calls; a custom range merges at most 7 day files.
# fetch_historical_cells() above is only the fallback for when the `cells`
# branch doesn't exist yet. File format: see build_cells.py.
CELLS_BASE = f"https://raw.githubusercontent.com/{SCRAPE_REPO}/cells"
CELLS_MAGIC = b"CELLS1\n"
CELLS_CHECK_SEC = 300   # how often to ask GitHub whether a cells file changed
_cells_state = {}       # path -> {"etag": str or None, "checked": monotonic time}


def _fetch_cells_file(path, immutable=False):
    """(local Path or None, error or None) for one file on the cells branch,
    re-checked with a conditional GET at most every CELLS_CHECK_SEC (never,
    for finished days). (None, None) means the file doesn't exist."""
    local = HEATMAP_DISK_CACHE_DIR / "cells" / path
    st = _cells_state.get(path)
    now = time.monotonic()
    if st is not None and local.exists() and (immutable or now - st["checked"] < CELLS_CHECK_SEC):
        return local, None
    if st is not None and st.get("missing") and now - st["checked"] < CELLS_CHECK_SEC:
        return None, None
    headers = dict(RAW_HEADERS)
    if st is not None and st.get("etag") and local.exists():
        headers["If-None-Match"] = st["etag"]
    url = f"{CELLS_BASE}/{path}"
    try:
        r = requests.get(url, headers=headers, timeout=30, stream=True)
        if r.status_code in RETRYABLE_STATUS:
            print(f"[cells] GET {path} -> HTTP {r.status_code}, retrying once", flush=True)
            wait = _retry_after_sec(r)
            r.close()
            time.sleep(wait)
            r = requests.get(url, headers=headers, timeout=30, stream=True)
        with r:
            print(f"[cells] GET {path} -> HTTP {r.status_code}", flush=True)
            if r.status_code == 304:
                st["checked"] = now
                return local, None
            if r.status_code == 404:
                _cells_state[path] = {"etag": None, "checked": now, "missing": True}
                return None, None
            r.raise_for_status()
            local.parent.mkdir(parents=True, exist_ok=True)
            tmp = local.with_name(f"{local.name}.{threading.get_ident()}.tmp")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(256 * 1024):
                    f.write(chunk)
            os.replace(tmp, local)
            _cells_state[path] = {"etag": r.headers.get("ETag"), "checked": now}
            return local, None
    except (requests.RequestException, OSError) as e:
        if local.exists():
            if st is not None:
                st["checked"] = now - CELLS_CHECK_SEC + HEATMAP_RETRY_AFTER_FAILURE_SEC
            return local, f"{path}: {e} (used the copy from an earlier fetch)"
        return None, f"{path}: {e}"


def _read_cells_file(local, keep_keys=False):
    """(header, {period: (CellGrid, keys or None)}) from a build_cells.py file."""
    with gzip.open(local, "rb") as f:
        data = f.read()
    if not data.startswith(CELLS_MAGIC):
        raise ValueError(f"{local.name}: not a cells file")
    mv = memoryview(data)
    off = len(CELLS_MAGIC)
    (hlen,) = struct.unpack_from("<I", data, off)
    off += 4
    header = json.loads(bytes(mv[off:off + hlen]))
    off += hlen
    grids = {}
    for p in header["periods"]:
        n = int(header["counts"][p])
        keys = array("q")
        keys.frombytes(mv[off:off + 8 * n])
        off += 8 * n
        g = CellGrid()
        g.index = None
        g.cols = []
        for _ in range(_CELL_COLS):
            col = array("d")
            col.frombytes(mv[off:off + 8 * n])
            off += 8 * n
            g.cols.append(col)
        grids[p] = (g, keys if keep_keys else None)
    if off != len(data):
        raise ValueError(f"{local.name}: {len(data) - off} trailing bytes")
    return header, grids


def _cells_meta(cells, agency_stats, files, rows, errors, lost, version, data_end=None):
    for p in VALID_HEATMAP_PERIODS:
        cells.setdefault(p, CellGrid().freeze())
    return {
        "cells": cells,
        "agency_stats": agency_stats,
        "files_fetched": files,
        "rows_seen_total": rows,
        "last_error": errors[-1] if errors else None,
        "n_errors": len(errors),
        "files_lost": lost,
        "deadline_hit": False,
        "data_end": data_end,
        "version": version,
        "source": "cells",
    }


def cells_from_prebuilt(window, previous=None):
    """Window meta (same shape as fetch_historical_cells) from the cells
    branch, or None if it isn't available so the caller falls back to the
    raw CSVs. Returns `previous` unchanged when the file hasn't changed."""
    start = time.monotonic()
    if not window.is_custom:
        local, err = _fetch_cells_file(f"windows/{window.hours}.bin.gz")
        if local is None:
            if err is None:
                return None  # 404: no cells branch yet, so parse the CSVs instead
            # GitHub refused or timed out (typically 429 right after a
            # restart, when the disk copy is gone). Do NOT fall back to
            # parsing a week of CSVs here: that's the CPU load that used to
            # starve the instance. Report it and retry in a minute.
            print(f"[cells] {err}; will retry", flush=True)
            return _cells_meta({}, {}, 0, 0, [err], 1, "error")
        version = f"{local.stat().st_mtime_ns}"
        if previous is not None and previous.get("version") == version and not err:
            return previous
        header, grids = _read_cells_file(local)
        meta = _cells_meta({p: g for p, (g, _) in grids.items()}, header.get("agency_stats", {}),
                           header.get("files", 0), header.get("rows", 0), [err] if err else [], 0,
                           version, header.get("data_end"))
    else:
        today = datetime.now(tz=SYDNEY_TZ).date()
        merged = _new_period_cells()
        agency = defaultdict(lambda: [0, 0, 0, 0, 0.0])
        files = rows = lost = found = 0
        errors, parts = [], []
        d = window.start_date
        while d <= window.end_date:
            local, err = _fetch_cells_file(f"days/{d.isoformat()}.bin.gz",
                                           immutable=d < today - timedelta(days=HEATMAP_MUTABLE_DAYS))
            if err:
                errors.append(err)
            if local is None:
                lost += err is not None
            else:
                found += 1
                header, grids = _read_cells_file(local, keep_keys=True)
                for p, (g, keys) in grids.items():
                    if p in merged:
                        merged[p].add_cells(keys, g.cols)
                for aid, st in header.get("agency_stats", {}).items():
                    acc = agency[aid]
                    for i in range(5):
                        acc[i] += st[i]
                files += header.get("files", 0)
                rows += header.get("rows", 0)
                parts.append(f"{d}:{local.stat().st_mtime_ns}")
            d += timedelta(days=1)
        if not found and not errors and "windows/168.bin.gz" not in _cells_state:
            # Can't tell "no data those days" from "no cells branch": check.
            if _fetch_cells_file("windows/168.bin.gz")[0] is None:
                return None
        for g in merged.values():
            g.freeze()
        meta = _cells_meta(merged, dict(agency), files, rows, errors, lost, "|".join(parts))
    cells = meta["cells"]["all"]
    print(f"[cells] window={window.key} cells={len(cells)} rows={meta['rows_seen_total']} "
          f"files={meta['files_fetched']} errors={meta['n_errors']} "
          f"data_end={meta['data_end']} elapsed={time.monotonic() - start:.2f}s", flush=True)
    _log_rss(f"cells window={window.key}")
    return meta


def compute_metric_points(cells, metric):
    return [_compact_point(p) for p in _compute_metric_points(cells, metric)]


def _compact_point(p):
    """Round a point for the wire: 5 dp of lat/lon is ~1 m (cells are 10 m),
    4 dp of a 0..1 value is far below a colour step, and sample counts are
    whole numbers. Full float reprs made the 7-day response ~6.5 MB."""
    out = [round(p[0], 5), round(p[1], 5), round(p[2], 4)]
    if len(p) > 3:
        out.append(int(p[3]) if p[3] >= 1 else round(p[3], 3))
    return out


def _compute_metric_points(cells, metric):
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
        eligible = [c for c in cells.rows() if c[2] > 0]
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
        for c in cells.rows():
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
        eligible = [c for c in cells.rows() if c[2] > 0]
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
        for c in cells.rows():
            if c[4] <= 0:
                continue
            off_time = c[N_OFF]
            points.append([c[0] / c[2], c[1] / c[2], (off_time / c[4]) / NOT_ON_TIME_CAP, c[4]])
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
            for c in cells.rows() if c[4] > 0]


def metric_prior(cells, metric):
    """[network value (normalised like the metric's points), SHRINK_PRIOR_N]
    for the metrics the client shrinks per pixel, else None."""
    n = sum(cells.cols[N_DELAY])
    if not n:
        return None
    if metric in ("delay", "delay_mean"):
        return [sum(cells.cols[LATE_SUM]) / n / DELAY_FIELD_CAP_SEC, SHRINK_PRIOR_N]
    if metric == "frequency":
        off = sum(cells.cols[N_OFF])
        return [off / n / NOT_ON_TIME_CAP, SHRINK_PRIOR_N]
    return None


_METRIC_LABELS = {
    "delay": "delay", "delay_mean": "mean delay",
    "delay_total": "delay", "frequency": "on-time",
    "density": "vehicle", "speed": "speed",
}


class WindowSpec:
    """A historical window: the last N hours (key = N), or a custom date
    range [start_date 00:00, end_date + 1 day 00:00) Sydney time
    (key = "YYYY-MM-DD..YYYY-MM-DD")."""

    def __init__(self, hours=None, start_date=None, end_date=None):
        self.hours, self.start_date, self.end_date = hours, start_date, end_date

    @property
    def key(self):
        return self.hours if self.hours is not None else f"{self.start_date}..{self.end_date}"

    @property
    def is_custom(self):
        return self.hours is None

    def bounds(self):
        if not self.is_custom:
            return None, None
        start = datetime.combine(self.start_date, datetime.min.time(), tzinfo=SYDNEY_TZ)
        end = datetime.combine(self.end_date + timedelta(days=1), datetime.min.time(), tzinfo=SYDNEY_TZ)
        return start, end

    def ttl(self):
        if self.is_custom:
            # A range that ended before today only changes if local data is
            # pushed for it; one that includes today keeps refreshing.
            today = datetime.now(tz=SYDNEY_TZ).date()
            return HEATMAP_WINDOW_CACHE_TTL_LONG_SECONDS if self.end_date < today else HEATMAP_WINDOW_CACHE_TTL_SECONDS
        return HEATMAP_WINDOW_CACHE_TTL_LONG_SECONDS if self.hours >= 168 else HEATMAP_WINDOW_CACHE_TTL_SECONDS

    def describe(self):
        if self.is_custom:
            return f"{self.start_date:%d %b %Y} to {self.end_date:%d %b %Y}"
        return {1: "last hour", 24: "last 24 hours", 168: "last 7 days"}.get(
            self.hours, f"last {self.hours} hours")


# Custom ranges: at most this many days, starting no earlier than the scrape
# keeps data for (gtfs-r-scrape COLLECTOR_RETENTION_DAYS = 60).
CUSTOM_WINDOW_MAX_DAYS = 7  # same cap as the longest preset window
CUSTOM_WINDOW_LOOKBACK_DAYS = 62
CUSTOM_CACHE_MAX_ENTRIES = 4  # bound memory: each cached window holds its cells


def parse_window_args(args):
    """WindowSpec from ?window=1|24|168 or ?window=custom&start=&end=
    (YYYY-MM-DD, inclusive). Returns (spec, error_message_or_None)."""
    w = args.get("window", str(HEATMAP_DEFAULT_WINDOW))
    if w != "custom":
        try:
            hours = int(w)
        except ValueError:
            hours = HEATMAP_DEFAULT_WINDOW
        return WindowSpec(hours if hours in HEATMAP_WINDOWS else HEATMAP_DEFAULT_WINDOW), None
    try:
        start = datetime.strptime(args.get("start", ""), "%Y-%m-%d").date()
        end = datetime.strptime(args.get("end", ""), "%Y-%m-%d").date()
    except ValueError:
        return None, "Custom range needs a start and end date"
    today = datetime.now(tz=SYDNEY_TZ).date()
    if end > today:
        end = today
    if start > end:
        return None, "Start date is after the end date"
    if (end - start).days + 1 > CUSTOM_WINDOW_MAX_DAYS:
        return None, f"Custom range can be at most {CUSTOM_WINDOW_MAX_DAYS} days"
    if start < today - timedelta(days=CUSTOM_WINDOW_LOOKBACK_DAYS):
        return None, f"Data is only kept for about {CUSTOM_WINDOW_LOOKBACK_DAYS} days"
    return WindowSpec(start_date=start, end_date=end), None


# A build that lost files (429/5xx with no disk copy) or hit the deadline is
# retried after this long instead of being cached for the window's full
# 5-30 minute TTL. Previously one burst of 429s blanked the map for up to
# 30 minutes.
HEATMAP_RETRY_AFTER_FAILURE_SEC = 60


def _is_fresh(entry, ttl, now):
    return (now - entry["fetched_at"]).total_seconds() < entry.get("ttl_override", ttl)


def _store_window(key, window, meta):
    if window.is_custom:
        custom_keys = [k for k in _heatmap_cells_cache if isinstance(k, str) and k != key]
        while len(custom_keys) >= CUSTOM_CACHE_MAX_ENTRIES:
            oldest = min(custom_keys, key=lambda k: _heatmap_cells_cache[k]["fetched_at"])
            _heatmap_cells_cache.pop(oldest, None)
            custom_keys.remove(oldest)
    _heatmap_cells_cache[key] = meta


def get_historical_cells_cached(window):
    if not isinstance(window, WindowSpec):
        window = WindowSpec(window)
    key, ttl = window.key, window.ttl()
    now = datetime.now(tz=SYDNEY_TZ)
    cached = _heatmap_cells_cache.get(key)
    if cached is not None and _is_fresh(cached, ttl, now):
        return cached
    if cached is not None:
        # Stale-while-revalidate: if another thread is already rebuilding,
        # serve the previous build now instead of queueing behind the lock.
        # Otherwise every open dashboard and the projector page would park
        # a gunicorn thread here for the whole rebuild (tens of seconds on
        # Render's free CPU), leaving none for /ping or /api/vehicles.
        if not _heatmap_lock.acquire(blocking=False):
            return cached
        _heatmap_lock.release()

    with _heatmap_lock:
        now = datetime.now(tz=SYDNEY_TZ)
        cached = _heatmap_cells_cache.get(key)
        if cached is not None and _is_fresh(cached, ttl, now):
            return cached

        meta = None
        try:
            meta = cells_from_prebuilt(window, cached)
        except Exception as e:  # a corrupt or unexpected file must not take the map down
            print(f"[cells] window={key}: {e!r}; falling back to raw CSVs", flush=True)
        if meta is not None:
            if meta is cached:  # file unchanged since the last check
                cached["fetched_at"] = now
                cached["ttl_override"] = CELLS_CHECK_SEC
                return cached
            meta["fetched_at"] = now
            if not window.is_custom:
                meta["ttl_override"] = CELLS_CHECK_SEC
            if meta["files_lost"]:
                meta["ttl_override"] = HEATMAP_RETRY_AFTER_FAILURE_SEC
                if cached is not None and cached["files_fetched"] > meta["files_fetched"]:
                    cached["fetched_at"] = now  # keep showing the last good build
                    cached["ttl_override"] = HEATMAP_RETRY_AFTER_FAILURE_SEC
                    return cached
            _store_window(key, window, meta)
            return meta
        start_dt, end_dt = window.bounds()
        meta = fetch_historical_cells(window.hours, start_dt, end_dt)
        meta["fetched_at"] = now
        meta["version"] = now.isoformat()
        if meta["deadline_hit"] or meta["files_lost"]:
            meta["ttl_override"] = HEATMAP_RETRY_AFTER_FAILURE_SEC
            if cached is not None and cached["files_fetched"] > meta["files_fetched"]:
                # Keep serving the last good build rather than a worse one;
                # try again in a minute.
                cached["fetched_at"] = now
                cached["ttl_override"] = HEATMAP_RETRY_AFTER_FAILURE_SEC
                print(f"[heatmap] window={key}: rebuild degraded "
                      f"({meta['files_fetched']} files, {meta['files_lost']} lost); "
                      f"keeping previous build", flush=True)
                return cached
        _store_window(key, window, meta)
        return meta


def get_heatmap_points_cached(window, metric, period="all"):
    if metric not in VALID_HEATMAP_METRICS:
        metric = "delay"
    if period not in VALID_HEATMAP_PERIODS:
        period = "all"

    # The window fetch already bucketed cells by period in its single pass;
    # picking a period here is just a dict lookup, never a fresh fetch.
    meta = get_historical_cells_cached(window)
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
    elif meta.get("files_lost") and meta.get("source") != "cells":
        note = (f"Partial data: {meta['files_lost']} day file(s) couldn't be downloaded "
                f"(GitHub said: {meta['last_error']}); retrying in a minute")
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
    // Text scale for map popups: the projector page sets PROJECTOR_UI_SCALE
    // (its CSS doubles the text) so popups get room to grow; 1 elsewhere.
    const UI_SCALE = (typeof PROJECTOR_UI_SCALE !== 'undefined') ? PROJECTOR_UI_SCALE : 1;
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
    // Selected bus's route and upcoming stops (see selectTrip below). Own
    // pane between the heat canvases (overlayPane, z 400) and the bus
    // markers (markerPane, z 600), so the line sits on top of the heat but
    // never hides a bus.
    map.createPane('tripPane');
    map.getPane('tripPane').style.zIndex = 450;
    const tripRenderer = L.svg({ pane: 'tripPane' });
    const tripLayer = L.layerGroup().addTo(map);
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
          <option value="168" selected>Last 7 days</option>
          <option value="custom">Custom dates\u2026</option>
        </select>
        <label class="hp-custom" for="histStartDate" hidden>From</label>
        <input class="hp-custom" type="date" id="histStartDate" hidden>
        <label class="hp-custom" for="histEndDate" hidden>To</label>
        <input class="hp-custom" type="date" id="histEndDate" hidden>
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

    // Custom history range: date inputs appear when "Custom dates…" is
    // picked. Bounds mirror the server's (CUSTOM_WINDOW_LOOKBACK_DAYS /
    // CUSTOM_WINDOW_MAX_DAYS = 7 days long, starting within the ~62 days
    // the scraper keeps).
    const isoDate = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    const startInput = heatPanel.querySelector('#histStartDate');
    const endInput = heatPanel.querySelector('#histEndDate');
    {
      const today = new Date(), earliest = new Date(), weekAgo = new Date();
      earliest.setDate(today.getDate() - 62);
      weekAgo.setDate(today.getDate() - 6);
      for (const el of [startInput, endInput]) { el.min = isoDate(earliest); el.max = isoDate(today); }
      startInput.value = isoDate(weekAgo);
      endInput.value = isoDate(today);
    }
    function syncCustomVisibility() {
      const custom = heatPanel.querySelector('#histWindowSelect').value === 'custom';
      heatPanel.querySelectorAll('.hp-custom').forEach(el => { el.hidden = !custom; });
    }
    function historyQuery() {
      const wh = heatPanel.querySelector('#histWindowSelect').value;
      if (wh !== 'custom') return { query: `window=${wh}` };
      if (!startInput.value || !endInput.value) return { error: 'Pick a start and end date' };
      if (startInput.value > endInput.value) return { error: 'Start date is after the end date' };
      return { query: `window=custom&start=${startInput.value}&end=${endInput.value}` };
    }

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
      const hq = historyQuery();
      const period = document.getElementById('histPeriodSelect').value;
      const metricAtRequest = currentMetric;
      statusDiv.textContent = 'Loading…';
      statusDiv.style.color = '#666';
      // Visible on the map itself (not just the collapsed layer control)
      // so a slow cold-cache fetch reads as "still working" rather than
      // "did switching this do anything?".
      heatLoadingBanner.textContent = `Updating heatmap: ${METRICS[metricAtRequest].label.toLowerCase()}\u2026`;
      heatLoadingBanner.hidden = false;
      if (hq.error) {
        statusDiv.textContent = hq.error; statusDiv.style.color = '#b3261e';
        heatLoadingBanner.hidden = true;
        return;
      }
      try {
        const res = await fetch(`/api/heatmap?${hq.query}&metric=${metricAtRequest}&period=${period}`);
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
        else {
          // Preset windows end at the newest collected snapshot (the
          // scraper pushes roughly hourly), so say how current they are.
          const asOf = data.data_end ? ` \u00b7 data to ${new Date(data.data_end).toLocaleString('en-AU', {weekday: 'short', hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Australia/Sydney'})}` : '';
          statusDiv.textContent = `${points.length.toLocaleString('en-AU')} cells loaded${asOf}`; statusDiv.style.color = '';
        }
      } catch (e) {
        if (seq !== histRequestSeq) return;
        statusDiv.textContent = 'Fetch failed: ' + e; statusDiv.style.color = '#b3261e';
      } finally {
        if (seq === histRequestSeq) heatLoadingBanner.hidden = true;
      }
    }
    document.getElementById('histWindowSelect').addEventListener('change', () => { syncCustomVisibility(); loadHistoricalHeatmap(); });
    startInput.addEventListener('change', loadHistoricalHeatmap);
    endInput.addEventListener('change', loadHistoricalHeatmap);
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
      // Delay metrics use each vehicle's own lateness, not one point per
      // vehicle — otherwise the heat layer just draws the route
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
      return `<strong>${rl}</strong><br>${v.route_operator || 'Unknown operator'}<br>Trip ${v.trip_id ?? '?'}<br>${dt}<br>${sk}` + tripSummaryHtml(v);
    }

    // ---- Click a bus: draw its route and upcoming stops ----
    // /api/trip returns the trip's timetable shape (from gtfs-r-scrape's
    // weekly lookup) and every stop the live feed still lists for it, each
    // with its predicted delay. The line is split where the bus is now:
    // faded dashes behind it, solid ahead. Stops are coloured by the same
    // TfNSW on-time window as the markers, so you can see whether the
    // bus's lateness is expected to grow or recover before it terminates.
    // Re-fetched on every vehicle poll while the popup is open.
    const ON_TIME_EARLY_SEC = {{ on_time_early_sec }}, ON_TIME_LATE_SEC = {{ on_time_late_sec }};
    const STATUS_LATE = '{{ status_late }}', STATUS_EARLY = '{{ status_early }}', STATUS_ON_TIME = '{{ status_on_time }}';
    const ROUTE_AHEAD = '#1f2a44', ROUTE_BEHIND = '#5b6474';
    let selectedKey = null, selectedTripId = null, tripDetail = null, tripReq = 0;
    let tripGeom = null;  // {ahead, behind, stops} as [lat, lon] lists, for popup placement

    function escHtml(t) {
      return String(t).replace(/[&<>"']/g, ch => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[ch]));
    }
    function fmtDelay(sec) {
      if (sec == null) return 'no prediction';
      const m = Math.round(sec / 6) / 10;
      return m === 0 ? 'on time' : `${m > 0 ? '+' : ''}${m} min`;
    }
    function stopColour(sec) {
      if (sec == null) return '#888';
      if (sec < ON_TIME_EARLY_SEC) return STATUS_EARLY;
      if (sec > ON_TIME_LATE_SEC) return STATUS_LATE;
      return STATUS_ON_TIME;
    }
    function stopLabel(s) { return s.name || `Stop ${s.stop_id}`; }

    // Popup lines for the selected trip: next stop, and the last stop the
    // feed predicts for — the bus's delay "outlook" along the route.
    function tripSummaryHtml(v) {
      if (!v.trip_id || v.trip_id !== selectedTripId) return '';
      if (!tripDetail) return '<div class="trip-summary trip-muted">Loading route&hellip;</div>';
      if (tripDetail.load_error) return `<div class="trip-summary trip-muted">Route unavailable (${escHtml(tripDetail.load_error)}); retrying&hellip;</div>`;
      const st = tripDetail.stops || [];
      let html = '<div class="trip-summary">';
      if (st.length) {
        const next = st[0], last = st[st.length - 1];
        html += `Next: ${escHtml(stopLabel(next))} <span class="trip-delay" style="color:${stopColour(next.delay_sec)}">${fmtDelay(next.delay_sec)}</span>`;
        if (st.length > 1) {
          html += `<br>${st.length - 1} more stop${st.length > 2 ? 's' : ''} to ${escHtml(stopLabel(last))} <span class="trip-delay" style="color:${stopColour(last.delay_sec)}">${fmtDelay(last.delay_sec)}</span>`;
          if (next.delay_sec != null && last.delay_sec != null) {
            const ch = (last.delay_sec - next.delay_sec) / 60;
            if (Math.abs(ch) >= 0.5) html += `<br><span class="trip-muted">Predicted to ${ch > 0 ? 'lose' : 'recover'} ${Math.abs(Math.round(ch * 10) / 10)} min by then</span>`;
          }
        }
      } else {
        html += '<span class="trip-muted">No upcoming stops in the live feed</span>';
      }
      if (tripDetail.shape_error) html += `<br><span class="trip-muted">Route line: ${escHtml(tripDetail.shape_error.toLowerCase())}</span>`;
      return html + '</div>';
    }

    // Index of the shape vertex nearest the bus (equirectangular distance
    // is plenty at this scale), so the line can be split there.
    function nearestVertex(pts, lat, lon) {
      const k = Math.cos(lat * Math.PI / 180);
      let best = 0, bd = Infinity;
      for (let i = 0; i < pts.length; i++) {
        const dy = pts[i][0] - lat, dx = (pts[i][1] - lon) * k, d = dx * dx + dy * dy;
        if (d < bd) { bd = d; best = i; }
      }
      return best;
    }
    function drawTrip(detail, v) {
      tripLayer.clearLayers();
      tripGeom = { ahead: [], behind: [], stops: [] };
      if (detail.shape && detail.shape.length >= 4) {
        const flat = decodeRouteLines([detail.shape])[0].pts;
        const pts = [];
        for (let i = 0; i < flat.length; i += 2) pts.push([flat[i], flat[i + 1]]);
        const cut = (v && v.lat != null) ? nearestVertex(pts, v.lat, v.lon) : 0;
        const behind = pts.slice(0, cut + 1), ahead = pts.slice(cut);
        tripGeom.ahead = ahead; tripGeom.behind = behind;
        const opts = { renderer: tripRenderer, interactive: false, lineCap: 'round', lineJoin: 'round' };
        if (behind.length > 1) L.polyline(behind, { ...opts, color: ROUTE_BEHIND, weight: 3, opacity: 0.55, dashArray: '4 6' }).addTo(tripLayer);
        if (ahead.length > 1) {
          // White casing under the line keeps it legible over the darkest
          // heat colours and on the projected model.
          L.polyline(ahead, { ...opts, color: '#fff', weight: 8, opacity: 0.85 }).addTo(tripLayer);
          L.polyline(ahead, { ...opts, color: ROUTE_AHEAD, weight: 4, opacity: 0.95 }).addTo(tripLayer);
        }
      }
      (detail.stops || []).forEach((s, i) => {
        if (s.lat == null || s.lon == null) return;
        tripGeom.stops.push([s.lat, s.lon]);
        L.circleMarker([s.lat, s.lon], {
          renderer: tripRenderer, radius: i === 0 ? 7 : 5, weight: 2, color: '#fff',
          fillColor: stopColour(s.delay_sec), fillOpacity: 1,
        }).bindTooltip(`<strong>${escHtml(stopLabel(s))}</strong><br>${i === 0 ? 'Next stop &middot; ' : ''}${fmtDelay(s.delay_sec)}`,
                       { direction: 'top', offset: [0, -6], className: 'glass-tooltip' })
          .addTo(tripLayer);
      });
    }

    // ---- Keep the bus info popup off the selected route ----
    // Leaflet always puts a popup directly above its marker, which is
    // usually right on top of the route line and the next few stops. Once
    // the popup is open (and again whenever its content, the route or the
    // view changes) we try eight positions around the bus and keep the one
    // that covers the least of the route ahead (route behind and stops are
    // weighted too), stays inside the visible map (the model box in
    // projector mode) and doesn't sit under the menu. Scored in container
    // pixels; the menu test maps points through the projector warp.
    const POPUP_GAP = 8, MARKER_HW = 28, MARKER_HH = 12;
    // Leaflet geometry: the popup's content box ends 20 px (tip) above
    // the anchor, and the anchor is popupAnchor = 12 px above the marker.
    const POPUP_TIP_DROP = 32;
    function popupCandidates(W, H) {
      const vx = MARKER_HW + POPUP_GAP + W / 2, vy = MARKER_HH + POPUP_GAP + H / 2;
      const dx = W / 2 + POPUP_GAP;
      return [
        { name: 'top',          dx: 0,   dy: -(H / 2 + POPUP_TIP_DROP), bias: 0 },
        { name: 'bottom',       dx: 0,   dy: vy,  bias: 4 },
        { name: 'right',        dx: vx,  dy: 0,   bias: 6 },
        { name: 'left',         dx: -vx, dy: 0,   bias: 6 },
        { name: 'top-right',    dx: dx,  dy: -vy, bias: 8 },
        { name: 'top-left',     dx: -dx, dy: -vy, bias: 8 },
        { name: 'bottom-right', dx: dx,  dy: vy,  bias: 9 },
        { name: 'bottom-left',  dx: -dx, dy: vy,  bias: 9 },
      ];
    }
    // Length of segment a-b inside rect r (Liang-Barsky clip).
    function clipLen(ax, ay, bx, by, r) {
      let t0 = 0, t1 = 1;
      const dx = bx - ax, dy = by - ay;
      const p = [-dx, dx, -dy, dy], q = [ax - r.x0, r.x1 - ax, ay - r.y0, r.y1 - ay];
      for (let i = 0; i < 4; i++) {
        if (p[i] === 0) { if (q[i] < 0) return 0; continue; }
        const t = q[i] / p[i];
        if (p[i] < 0) { if (t > t1) return 0; if (t > t0) t0 = t; }
        else { if (t < t0) return 0; if (t < t1) t1 = t; }
      }
      return (t1 - t0) * Math.hypot(dx, dy);
    }
    function polyLenIn(pts, r) {
      let len = 0;
      for (let i = 1; i < pts.length; i++) len += clipLen(pts[i - 1].x, pts[i - 1].y, pts[i].x, pts[i].y, r);
      return len;
    }
    // The part of the map container that's actually seen: the model box
    // in projector mode, else the whole container.
    function popupViewRect() {
      if (typeof PROJECT_BOUNDS !== 'undefined') {
        const a = map.latLngToContainerPoint(PROJECT_BOUNDS.getNorthWest());
        const b = map.latLngToContainerPoint(PROJECT_BOUNDS.getSouthEast());
        return { x0: a.x, y0: a.y, x1: b.x, y1: b.y };
      }
      const sz = map.getSize();
      return { x0: 0, y0: 0, x1: sz.x, y1: sz.y };
    }
    // Container px -> screen px, through the projector's perspective warp
    // if there is one, so we can test against the (unwarped) menu.
    function containerToScreenFn() {
      const el = map.getContainer();
      const t = getComputedStyle(el).transform;
      if (!t || t === 'none') {
        const r = el.getBoundingClientRect();
        return (x, y) => [r.left + x, r.top + y];
      }
      const m = new DOMMatrix(t), ox = el.offsetLeft, oy = el.offsetTop;
      return (x, y) => { const p = m.transformPoint(new DOMPoint(x, y, 0, 1)); return [p.x / p.w + ox, p.y / p.w + oy]; };
    }
    function menuRects() {
      return Array.from(document.querySelectorAll('.leaflet-control-container .leaflet-control, #projControls'))
        .map(e => e.getBoundingClientRect()).filter(r => r.width > 0 && r.height > 0);
    }
    function placePopup(m) {
      const popup = m && m.getPopup();
      if (!popup || !popup.isOpen()) return;
      const el = popup.getElement();
      const wrap = el && el.querySelector('.leaflet-popup-content-wrapper');
      if (!wrap) return;
      const W = wrap.offsetWidth, H = wrap.offsetHeight;
      const c = map.latLngToContainerPoint(m.getLatLng());
      const toPx = ll => map.latLngToContainerPoint(ll);
      const g = tripGeom || { ahead: [], behind: [], stops: [] };
      const ahead = g.ahead.map(toPx), behind = g.behind.map(toPx), stops = g.stops.map(toPx);
      const view = popupViewRect(), toScreen = containerToScreenFn(), menus = menuRects();
      const pad = 6;
      function scoreOf(cand) {
        const cx = c.x + cand.dx, cy = c.y + cand.dy;
        const r = { x0: cx - W / 2 - pad, x1: cx + W / 2 + pad, y0: cy - H / 2 - pad, y1: cy + H / 2 + pad };
        let score = cand.bias + polyLenIn(ahead, r) + 0.35 * polyLenIn(behind, r);
        for (const s of stops) if (s.x >= r.x0 && s.x <= r.x1 && s.y >= r.y0 && s.y <= r.y1) score += 30;
        // Off the visible map: share of the box outside it.
        const ix = Math.max(0, Math.min(r.x1, view.x1) - Math.max(r.x0, view.x0));
        const iy = Math.max(0, Math.min(r.y1, view.y1) - Math.max(r.y0, view.y0));
        score += 5000 * (1 - (ix * iy) / ((r.x1 - r.x0) * (r.y1 - r.y0)));
        // Under the menu: sample a grid of points across the box.
        if (menus.length) {
          let hit = 0, n = 0;
          for (let i = 0; i <= 4; i++) for (let j = 0; j <= 3; j++) {
            const [sx, sy] = toScreen(r.x0 + (r.x1 - r.x0) * i / 4, r.y0 + (r.y1 - r.y0) * j / 3);
            n++;
            if (menus.some(mr => sx >= mr.left && sx <= mr.right && sy >= mr.top && sy <= mr.bottom)) hit++;
          }
          score += 3000 * hit / n;
        }
        return score;
      }
      const scored = popupCandidates(W, H).map(cand => ({ ...cand, score: scoreOf(cand) }));
      let chosen = scored.reduce((x, y) => (y.score < x.score ? y : x));
      // Small hysteresis so the popup doesn't hop sides on every poll.
      const cur = scored.find(cd => cd.name === m._popupPlace);
      if (cur && cur.score <= chosen.score + 15) chosen = cur;
      m._popupPlace = chosen.name;
      L.DomUtil[chosen.name === 'top' ? 'removeClass' : 'addClass'](el, 'popup-side');
      popup.options.offset = L.point(Math.round(chosen.dx), Math.round(chosen.dy + H / 2 + POPUP_TIP_DROP));
      // Reposition without letting autoPan drag the map around (it would
      // fight the placement, and must never move the projector view).
      const autoPan = popup.options.autoPan;
      popup.options.autoPan = false;
      popup.update();
      popup.options.autoPan = autoPan;
    }
    function placeSelectedPopup() {
      if (selectedKey != null) placePopup(markers.get(selectedKey));
    }
    map.on('zoomend moveend resize', placeSelectedPopup);
    // One /api/trip request at a time: the selected bus is re-fetched on
    // every 15 s poll (its upcoming stops change), and a slow response used
    // to stack a new request on top of each unanswered one.
    let tripInFlight = false;
    function refreshSelectedPopup(key) {
      const v = lastVehicles.find(x => (x.vehicle_id || x.trip_id) === key);
      const m = markers.get(key);
      if (m && v && m.isPopupOpen()) { m.getPopup().setContent(popupContent(v)); placePopup(m); }
      return v;
    }
    async function loadTrip() {
      const tripId = selectedTripId, key = selectedKey;
      if (!tripId || tripInFlight) return;
      const req = ++tripReq;
      tripInFlight = true;
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), 20000);
      try {
        const res = await fetch('/api/trip/' + encodeURIComponent(tripId), { signal: ctl.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const detail = await res.json();
        if (req !== tripReq || tripId !== selectedTripId) return;  // superseded by a newer click/poll
        tripDetail = detail;
        drawTrip(detail, refreshSelectedPopup(key));
      } catch (e) {
        console.warn('Trip load failed', e);
        if (req !== tripReq || tripId !== selectedTripId) return;
        // Keep a route we already drew; otherwise say why it's missing.
        if (!tripDetail || tripDetail.load_error) {
          tripDetail = { load_error: e.name === 'AbortError' ? 'server slow to respond' : String(e.message || e) };
          refreshSelectedPopup(key);
        }
      } finally {
        clearTimeout(timer);
        tripInFlight = false;
      }
    }
    function selectTrip(key, v) {
      if (!v.trip_id) return;
      if (selectedKey !== key || selectedTripId !== v.trip_id) {
        tripDetail = tripGeom = null; tripLayer.clearLayers();
        tripInFlight = false;  // a request for the previous bus is now irrelevant
      }
      selectedKey = key; selectedTripId = v.trip_id;
      loadTrip();
    }
    function clearTrip(key) {
      if (key != null && key !== selectedKey) return;  // a different popup closing
      selectedKey = selectedTripId = tripDetail = tripGeom = null;
      tripReq++;
      tripLayer.clearLayers();
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
            .bindPopup(popup, { className:'glass-popup', maxWidth: 300 * UI_SCALE,
                                autoPan: typeof PROJECT_BOUNDS === 'undefined' })
            .bindTooltip(tooltip, { direction:'top', offset:[0,-20], className:'glass-tooltip' });
          m.on('popupopen', () => {
            m._popupPlace = null;
            const cur = lastVehicles.find(x => (x.vehicle_id || x.trip_id) === key);
            if (cur) {
              selectTrip(key, cur);
              // The popup HTML was built before this bus was selected, so it
              // had no route section until the next poll; rebuild it now.
              m.getPopup().setContent(popupContent(cur));
            }
            placePopup(m);
          });
          m.on('popupclose', () => clearTrip(key));
          markers.set(key, m);
        }
      });
      for (const [key, m] of markers) { if (!seen.has(key)) { markersLayer.removeLayer(m); markers.delete(key); } }
      updateLiveHeatFromVehicles(vehicles);
      // Keep the selected bus's route/stops current: it may have passed a
      // stop or changed trip (end of run) since the last poll.
      if (selectedKey != null) {
        const v = vehicles.find(x => (x.vehicle_id || x.trip_id) === selectedKey);
        if (!v || !v.trip_id) clearTrip();
        else { selectTrip(selectedKey, v); placeSelectedPopup(); }
      }
    }
    // Skip a poll while the previous one is still waiting, and give up on
    // a request after 20 s. Without this, a slow server got a new request
    // every 15 s on top of the unanswered ones, from every open tab, which
    // tied up all six server threads and made the slowdown worse.
    let vehiclePollInFlight = false;
    async function pollVehicles() {
      if (vehiclePollInFlight) return;
      vehiclePollInFlight = true;
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), 20000);
      try {
        const res = await fetch('/api/vehicles' + window.location.search, { signal: ctl.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        renderVehicles(data.vehicles || []);
      } catch (e) { console.warn('Vehicle poll failed', e); }
      finally { clearTimeout(timer); vehiclePollInFlight = false; }
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
  :root { --bg:#fff; --text:#111; --muted:#666; --line:#ccc; --late:#b3261e; --early:#1e6b3c; --fill-blue: {{ color_fill }}; --sans: Helvetica, Arial, sans-serif; }
  * { box-sizing: border-box; }
  body { background:var(--bg); color:var(--text); font-family:var(--sans); margin:0; padding:24px 32px 60px; }
  h1 { font-size:1.4rem; font-weight:bold; border-bottom:2px solid var(--text); padding-bottom:10px; margin-bottom:4px; }
  .meta { color:var(--muted); font-size:0.85rem; margin-bottom:24px; }
  .meta a { color:#555; }
  h2 { font-size:1rem; font-weight:bold; margin-top:36px; border-bottom:1px solid var(--line); padding-bottom:4px; }
  table { border-collapse:collapse; width:100%; margin-top:10px; }
  th, td { padding:6px 12px; text-align:right; }
  th:first-child, td:first-child { text-align:left; }
  th { color:var(--muted); font-weight:normal; font-size:0.8rem; border-bottom:1px solid var(--line); }
  th a { color:inherit; text-decoration:underline; }
  th a:hover { color:var(--late); }
  th.help { text-decoration:underline dotted; text-underline-offset:2px; cursor:help; }
  /* Sortable column headers: click the header to sort; the arrow shows
     the active column and direction, a faint double arrow the others. */
  th.sortable { padding:0; }
  th .sort-btn { all:unset; box-sizing:border-box; display:flex; align-items:center; justify-content:flex-end; gap:4px; width:100%; padding:6px 12px; cursor:pointer; white-space:nowrap; }
  th:first-child .sort-btn { justify-content:flex-start; }
  th.help .sort-btn .lbl { text-decoration:underline dotted; text-underline-offset:2px; }
  th .sort-btn:hover, th .sort-btn:focus-visible { color:var(--text); }
  th .sort-btn:focus-visible { outline:2px solid var(--fill-blue); outline-offset:-2px; }
  th .sort-btn .arr { display:inline-block; width:0.8em; text-align:center; font-size:0.75rem; opacity:0.35; }
  th[aria-sort] .sort-btn { color:var(--text); }
  th[aria-sort] .sort-btn .arr { opacity:1; }
  .table-note { color:var(--muted); font-size:0.8rem; margin-top:6px; }
  tr:hover { background:#f4f4f4; }
  td.late { color:var(--late); }
  td.early { color:var(--early); }
  .flag { color:var(--muted); font-size:0.75rem; }
  .toggle { color:#555; text-decoration:underline; font-size:0.85rem; }
  #dashmap { height:600px; border:1px solid var(--line); margin-top:10px; background:#eee; }
  .map-legend { display:flex; gap:16px; align-items:center; font-size:0.8rem; color:var(--muted); margin-top:8px; flex-wrap:wrap; }
  .map-legend .swatch { display:inline-block; width:12px; height:12px; border-radius:50%; margin-right:4px; vertical-align:middle; background:var(--fill-blue); border:2px solid #999; }
  .map-error { color:var(--late); font-size:0.85rem; margin-top:8px; }
  .bus-marker { position:relative; width:56px; height:24px; }
  .bus-pill { position:absolute; top:0; left:50%; transform:translateX(-50%); display:flex; align-items:center; gap:4px; background:var(--fill-blue); color:#fff; font:600 11px/1 -apple-system, Helvetica, Arial, sans-serif; padding:5px 7px; border-radius:7px; border:2.5px solid #888; box-shadow:0 1px 3px rgba(0,0,0,0.4); white-space:nowrap; }
  .bus-arrow { flex:0 0 auto; font-size:12px; line-height:1; display:inline-block; color:#fff; }
  .bus-arrow-only { position:absolute; top:0; left:50%; transform:translateX(-50%); width:24px; height:24px; display:flex; align-items:center; justify-content:center; }
  .bus-arrow-only .arrow-glyph { display:inline-block; font-size:20px; line-height:1; text-shadow:0 0 2px #fff, 0 0 4px #fff, 0 1px 2px rgba(0,0,0,0.35); }
  .leaflet-popup-content { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif; }
  .leaflet-control-attribution a { color:#555; }
  .leaflet-control-attribution a[target="_blank"]::after { content:"\2197"; display:inline-block; padding-left:0.2em; }
  .glass-tooltip { background:rgba(255,255,255,0.55) !important; -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45) !important; border-radius:12px !important; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:7px 11px; }
  .glass-tooltip::before { display:none; }
  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper { background:rgba(255,255,255,0.55); -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45); border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; }
  .leaflet-popup.glass-popup .leaflet-popup-tip { background:rgba(255,255,255,0.55); box-shadow:none; }
  .leaflet-popup.popup-side .leaflet-popup-tip-container { display:none; }  /* moved off the route: no pointer */
  .trip-summary { margin-top:6px; padding-top:6px; border-top:1px solid rgba(0,0,0,0.12); }
  .trip-summary .trip-delay { font-weight:600; }
  .trip-muted { color:#666; }
  .leaflet-control-layers { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif !important; }
  .heat-panel { font:12px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#333; margin-top:6px; padding-top:6px; border-top:1px solid #ddd; width:230px; }
  .heat-panel .hp-section { font-size:10.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; color:#777; margin:8px 0 4px; }
  .heat-panel .hp-section:first-child { margin-top:0; }
  .heat-panel .hp-grid { display:grid; grid-template-columns:auto 1fr; gap:4px 8px; align-items:center; }
  .heat-panel .hp-grid label { color:#555; }
  .heat-panel select, .heat-panel input[type=date] { font:inherit; width:100%; min-width:0; box-sizing:border-box; }
  .heat-panel [hidden] { display:none !important; }
  .heat-panel .hp-inline { display:flex; align-items:center; gap:4px; min-width:0; }
  .heat-panel #routeMaskStatus { color:#888; white-space:nowrap; }
  .heat-panel #histWindowStatus { color:#777; font-size:11px; margin-top:4px; }
  .heat-panel .heat-legend { margin:8px 0 0; }
  .heat-panel .hp-desc { color:#666; font-size:11px; line-height:1.35; margin-top:6px; }
  .heat-help { margin:10px 0 0; font-size:0.85rem; color:#444; }
  .heat-help summary { cursor:pointer; font-weight:600; }
  .heat-help p { margin:8px 0; }
  .heat-help p, .heat-help dd, .heat-panel .hp-desc { text-align:justify; text-justify:inter-word; }
  .heat-help dl { margin:6px 0 0; display:grid; grid-template-columns:max-content 1fr; gap:4px 14px; }
  .heat-help dt { font-weight:600; }
  .heat-help dd { margin:0; }
  #histWindowStatus { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#b3261e; margin:2px 0 2px 22px; max-width:220px; }
  .heat-legend { font:11px/1.4 -apple-system, Helvetica, Arial, sans-serif; margin:6px 22px 2px; color:#333; }
  .heat-legend .heat-legend-title { font-weight:600; margin-bottom:2px; }
  .heat-legend .heat-legend-bar { height:10px; border-radius:2px; border:1px solid rgba(0,0,0,0.15); }
  .heat-legend .heat-legend-ticks { display:flex; justify-content:space-between; color:#888; margin-top:1px; }
  #heatLoadingBanner { position:absolute; top:10px; left:50%; transform:translateX(-50%); z-index:900; background:rgba(17,17,17,0.85); color:#fff; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; box-shadow:0 2px 8px rgba(0,0,0,0.25); pointer-events:none; }
  body.menu-hidden #dashmap .leaflet-control-layers { display:none !important; }
  .heat-panel .hp-hide { display:block; width:100%; margin-top:10px; font:inherit; cursor:pointer; }
  .menu-hint { position:absolute; left:50%; bottom:24px; transform:translateX(-50%); z-index:1000; background:rgba(17,17,17,0.85); color:#fff; font:600 13px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; opacity:0; transition:opacity 0.4s; pointer-events:none; }
  .menu-hint.show { opacity:1; }
</style>
</head>
<body>
  <h1>Delay Board <a class="toggle" style="float:right; font-size:0.8rem; font-weight:normal; border-bottom:none;" href="/project">Projector mode</a></h1>
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
    <p><strong>Live heatmap</strong> colours each bus by its current value. <strong>Historical heatmap</strong> uses every scraped reading in the chosen history window (default 7 days), optionally limited to a time of day. <strong>Route clip</strong> trims colour to within the chosen distance of a bus route, so results read along corridors.</p>
    <dl>
      <dt>Average delay</dt><dd>How late buses typically run in each area: mean lateness of readings within 250 m, with early running counted as on time (0). Where data is thin, the estimate is pulled toward the network average, so a single late bus can&rsquo;t create a hotspot. Scale 0&ndash;5 min.</dd>
      <dt>Delay burden</dt><dd>Where the most delay accumulates: total bus-minutes late, scaled to the same overall heat as Bus density. Busy corridors rank high even when each bus is only slightly late. An area hotter here than on Bus density carries more than its share of the network&rsquo;s delay.</dd>
      <dt>Not on time</dt><dd>Share of readings within 250 m that fall outside the TfNSW on-time window (0:59 early to 5:59 late), so early running counts as well as late. Thin data is pulled toward the network average. Scale 0&ndash;60%.</dd>
      <dt>Speed</dt><dd>Average bus speed within 160 m, including time stopped at stops and signals. Amber = CBD streets, pink = arterials, purple = motorways. Scale 0&ndash;80 km/h.</dd>
      <dt>Bus density</dt><dd>How many bus position readings were recorded in each area: where buses run most, not how well they run.</dd>
    </dl>
  </details>

  <h2>By operator</h2>
  <table class="sortable-table" data-sort-col="2" data-sort-dir="desc">
    <tr><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Operator</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Distinct buses reporting, using each bus’s latest stop. Anomalous readings (over an hour early or late) are excluded."><button type="button" class="sort-btn"><span class="lbl">n</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable" data-type="num" data-first="desc"><button type="button" class="sort-btn"><span class="lbl">avg delay</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Standard deviation (SD) of delay in minutes: how much delay varies between buses, not how late they run on average. Low = consistently delayed by about the same amount; high = some buses run much later (or earlier) than others."><button type="button" class="sort-btn"><span class="lbl">spread (&plusmn;min)</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="asc" title="Share of buses between 0:59 early and 5:59 late (TfNSW on-time running KPI window)."><button type="button" class="sort-btn"><span class="lbl">on time</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Earliest to latest bus. Sorts by the gap between them."><button type="button" class="sort-btn"><span class="lbl">range</span><span class="arr" aria-hidden="true"></span></button></th></tr>
    {% for r in operators %}
    <tr><td>{{ r.operator }}</td><td data-v="{{ r.n }}">{{ r.n }}</td>
      <td data-v="{{ r.mean_min }}" class="{{ 'late' if r.mean_min > 0 else 'early' }}">{{ '%+.1f'|format(r.mean_min) }} min</td>
      <td data-v="{{ r.stdev_min }}">{{ '%.1f'|format(r.stdev_min) }} min</td>
      <td data-v="{{ r.on_time_pct }}">{{ '%.0f'|format(r.on_time_pct) }}%</td>
      <td data-v="{{ r.max_min - r.min_min }}">{{ '%+.1f'|format(r.min_min) }} to {{ '%+.1f'|format(r.max_min) }} min</td></tr>
    {% endfor %}
  </table>

  <h2>By route</h2>
  <table class="sortable-table" data-limit="60" data-sort-col="{{ {'mean_min': 3, 'stdev_min': 4, 'on_time_pct': 5}.get(sort_key, 4) }}" data-sort-dir="{{ 'asc' if ascending else 'desc' }}">
    <tr><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Route</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Operator</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Distinct buses reporting, using each bus’s latest stop. Anomalous readings (over an hour early or late) are excluded."><button type="button" class="sort-btn"><span class="lbl">n</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable" data-type="num" data-first="desc"><button type="button" class="sort-btn"><span class="lbl">avg delay</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Standard deviation (SD) of delay in minutes: how much delay varies between buses, not how late they run on average. Low = consistently delayed by about the same amount; high = some buses run much later (or earlier) than others."><button type="button" class="sort-btn"><span class="lbl">spread (&plusmn;min)</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="asc" title="Share of buses between 0:59 early and 5:59 late (TfNSW on-time running KPI window)."><button type="button" class="sort-btn"><span class="lbl">on time</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Earliest to latest bus. Sorts by the gap between them."><button type="button" class="sort-btn"><span class="lbl">range</span><span class="arr" aria-hidden="true"></span></button></th></tr>
    {% for r in routes %}
    <tr><td>{{ r.route_num }}</td><td>{{ r.route_operator }}</td><td data-v="{{ r.n }}">{{ r.n }}</td>
      <td data-v="{{ r.mean_min }}" class="{{ 'late' if r.mean_min > 0 else 'early' }}">{{ '%+.1f'|format(r.mean_min) }} min</td>
      <td data-v="{{ r.stdev_min }}">{{ '%.1f'|format(r.stdev_min) }} min</td>
      <td data-v="{{ r.on_time_pct }}">{{ '%.0f'|format(r.on_time_pct) }}%</td>
      <td data-v="{{ r.max_min - r.min_min }}">{{ '%+.1f'|format(r.min_min) }} to {{ '%+.1f'|format(r.max_min) }} min</td></tr>
    {% endfor %}
  </table>
  {% if routes|length > 60 %}<div class="table-note">Showing the top 60 of {{ routes|length }} routes for the current sort. Click a column heading to sort; click again to reverse.</div>{% endif %}

  <h2>Individual trips &mdash; largest single delays</h2>
  <table class="sortable-table" data-sort-col="4" data-sort-dir="desc">
    <tr><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Trip</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Route</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Operator</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable" data-type="text" data-first="asc"><button type="button" class="sort-btn"><span class="lbl">Most recent stop</span><span class="arr" aria-hidden="true"></span></button></th><th class="sortable help" data-type="num" data-first="desc" title="Sorts by size of delay, early or late."><button type="button" class="sort-btn"><span class="lbl">delay</span><span class="arr" aria-hidden="true"></span></button></th></tr>
    {% for r in worst_trips[:30] %}
    <tr><td>{{ r.trip_id }}</td><td>{{ r.route_num }}</td><td>{{ r.route_operator }}</td><td>{{ r.stop_id }}</td>
      <td data-v="{{ (r.delay|abs) / 60 }}" class="{{ 'late' if r.delay > 0 else 'early' }}">{{ '%+.1f'|format(r.delay / 60) }} min{% if r.anomaly %} <span class="flag">(flagged)</span>{% endif %}</td></tr>
    {% endfor %}
  </table>

  <script>
  // Click-to-sort column headers (Wikipedia-style). Sorting happens in the
  // browser, so it doesn't reload the page or reset the map. Numeric cells
  // carry their raw value in data-v; text cells sort naturally ("M30" < "M50",
  // "301" < "1000"). data-limit shows only the top N rows after each sort.
  (function () {
    const ARROWS = { asc: '▲', desc: '▼', none: '⇅' };
    const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
    document.querySelectorAll('table.sortable-table').forEach(function (table) {
      const headers = Array.from(table.rows[0].cells);
      const limit = parseInt(table.dataset.limit || '0', 10);
      function cellKey(row, i, type) {
        const c = row.cells[i];
        if (type === 'num') {
          const v = parseFloat(c.dataset.v !== undefined ? c.dataset.v : c.textContent);
          return isNaN(v) ? -Infinity : v;
        }
        return c.textContent.trim();
      }
      function render(col, dir) {
        headers.forEach(function (th, i) {
          const arr = th.querySelector('.arr');
          if (!arr) return;
          if (i === col) {
            th.setAttribute('aria-sort', dir === 'asc' ? 'ascending' : 'descending');
            arr.textContent = ARROWS[dir];
          } else {
            th.removeAttribute('aria-sort');
            arr.textContent = ARROWS.none;
          }
        });
      }
      function sortBy(col, dir) {
        const type = headers[col].dataset.type;
        const body = table.rows[0].parentNode;
        const rows = Array.from(table.rows).slice(1);
        rows.sort(function (a, b) {
          const ka = cellKey(a, col, type), kb = cellKey(b, col, type);
          const r = type === 'num' ? ka - kb : collator.compare(ka, kb);
          return dir === 'asc' ? r : -r;
        });
        rows.forEach(function (row, i) {
          row.hidden = limit > 0 && i >= limit;
          body.appendChild(row);
        });
        table.dataset.sortCol = col;
        table.dataset.sortDir = dir;
        render(col, dir);
      }
      headers.forEach(function (th, i) {
        const btn = th.querySelector('.sort-btn');
        if (!btn) return;
        btn.addEventListener('click', function () {
          const same = parseInt(table.dataset.sortCol, 10) === i;
          const dir = same
            ? (table.dataset.sortDir === 'asc' ? 'desc' : 'asc')
            : (th.dataset.first || 'desc');
          sortBy(i, dir);
        });
      });
      sortBy(parseInt(table.dataset.sortCol || '0', 10), table.dataset.sortDir || 'desc');
    });
  })();
  </script>

  <script>
""" + render_map_script(
"""    const map = L.map('dashmap').setView([-33.8688, 151.2093], 11);
    map.attributionControl.setPrefix('<a href="https://leafletjs.com" target="_blank" rel="noopener">Leaflet</a>');"""
) + """

    // Hide the map menu for a clean view of the map, same as projector mode:
    // "Hide menu" button in the menu, M to toggle, ?menu=0 / ?menu=1 in the
    // URL to force it on load; otherwise the last choice is remembered in this
    // browser (separately from the projector page's setting).
    (function () {
      const STORE_KEY = 'dashboard.menuHidden';
      const params = new URLSearchParams(window.location.search);
      let hidden;
      if (params.has('menu')) hidden = params.get('menu') === '0';
      else { try { hidden = localStorage.getItem(STORE_KEY) === '1'; } catch (e) { hidden = false; } }
      const hint = document.createElement('div');
      hint.className = 'menu-hint';
      hint.textContent = 'Menu hidden \u2014 press M to show it';
      document.getElementById('dashmap').appendChild(hint);
      let hintTimer = null;
      function apply(announce) {
        document.body.classList.toggle('menu-hidden', hidden);
        try { localStorage.setItem(STORE_KEY, hidden ? '1' : '0'); } catch (e) { /* storage unavailable */ }
        clearTimeout(hintTimer);
        hint.classList.toggle('show', hidden && announce);
        if (hidden && announce) hintTimer = setTimeout(() => hint.classList.remove('show'), 2500);
      }
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'hp-hide';
      btn.textContent = 'Hide menu (M)';
      btn.addEventListener('click', () => { hidden = true; apply(true); });
      document.querySelector('.heat-panel').appendChild(btn);
      document.addEventListener('keydown', e => {
        if (e.key !== 'm' && e.key !== 'M') return;
        if (e.ctrlKey || e.metaKey || e.altKey) return;
        if (e.target.closest('input, select, textarea')) return;
        hidden = !hidden;
        apply(true);
      });
      apply(false);
    })();
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
  body { margin:0; background:#000; overflow:hidden; }
  #dashmap { position:absolute; inset:0; transform-origin:0 0; }
  .bus-marker { position:relative; width:56px; height:24px; }
  .bus-pill { position:absolute; top:0; left:50%; transform:translateX(-50%); display:flex; align-items:center; gap:4px; background:var(--fill-blue); color:#fff; font:600 11px/1 -apple-system, Helvetica, Arial, sans-serif; padding:5px 7px; border-radius:7px; border:2.5px solid #888; box-shadow:0 1px 3px rgba(0,0,0,0.4); white-space:nowrap; }
  .bus-arrow { flex:0 0 auto; font-size:12px; line-height:1; display:inline-block; color:#fff; }
  .bus-arrow-only { position:absolute; top:0; left:50%; transform:translateX(-50%); width:24px; height:24px; display:flex; align-items:center; justify-content:center; }
  .bus-arrow-only .arrow-glyph { display:inline-block; font-size:20px; line-height:1; text-shadow:0 0 2px #fff, 0 0 4px #fff, 0 1px 2px rgba(0,0,0,0.35); }
  .leaflet-popup-content { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif; }
  .leaflet-control-attribution a { color:#555; }
  .leaflet-control-attribution a[target="_blank"]::after { content:"\2197"; display:inline-block; padding-left:0.2em; }
  .glass-tooltip { background:rgba(255,255,255,0.55) !important; -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45) !important; border-radius:12px !important; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:7px 11px; }
  .glass-tooltip::before { display:none; }
  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper { background:rgba(255,255,255,0.55); -webkit-backdrop-filter:blur(14px) saturate(180%); backdrop-filter:blur(14px) saturate(180%); border:1px solid rgba(255,255,255,0.45); border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,0.18); color:#111; }
  .leaflet-popup.glass-popup .leaflet-popup-tip { background:rgba(255,255,255,0.55); box-shadow:none; }
  .leaflet-popup.popup-side .leaflet-popup-tip-container { display:none; }  /* moved off the route: no pointer */
  .trip-summary { margin-top:6px; padding-top:6px; border-top:1px solid rgba(0,0,0,0.12); }
  .trip-summary .trip-delay { font-weight:600; }
  .trip-muted { color:#666; }
  .leaflet-control-layers { font:13px/1.4 -apple-system, Helvetica, Arial, sans-serif !important; }
  .heat-panel { font:12px/1.4 -apple-system, Helvetica, Arial, sans-serif; color:#333; margin-top:6px; padding-top:6px; border-top:1px solid #ddd; width:230px; }
  .heat-panel .hp-section { font-size:10.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; color:#777; margin:8px 0 4px; }
  .heat-panel .hp-section:first-child { margin-top:0; }
  .heat-panel .hp-grid { display:grid; grid-template-columns:auto 1fr; gap:4px 8px; align-items:center; }
  .heat-panel .hp-grid label { color:#555; }
  .heat-panel select, .heat-panel input[type=date] { font:inherit; width:100%; min-width:0; box-sizing:border-box; }
  .heat-panel [hidden] { display:none !important; }
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
  #heatLoadingBanner { position:fixed; top:10px; left:50%; transform:translateX(-50%); z-index:1050; background:rgba(17,17,17,0.85); color:#fff; font:600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; box-shadow:0 2px 8px rgba(0,0,0,0.25); pointer-events:none; }
  /* Calibration: black everywhere outside the four corner crosshairs, the
     crosshairs themselves (calibration mode only) and the help box. */
  #projMask { position:fixed; inset:0; width:100%; height:100%; z-index:850; pointer-events:none; }
  #projControls { position:fixed; top:10px; right:10px; z-index:1100; }
  #projControls .leaflet-control-layers { margin:0; }
  .cal-handle { position:fixed; width:0; height:0; z-index:1200; cursor:move; display:none; }
  body.calibrating .cal-handle { display:block; }
  body.calibrating { user-select:none; -webkit-user-select:none; }
  body.calibrating #projControls { display:none; }  /* menu would cover the NE crosshair */
  .cal-handle svg { position:absolute; left:-40px; top:-40px; overflow:visible; }
  .cal-handle .cal-label { position:absolute; font:700 13px/1 -apple-system, Helvetica, Arial, sans-serif; color:#fff; background:#d0021b; padding:3px 6px; border-radius:4px; white-space:nowrap; }
  /* labels sit on the inward side of each corner so they stay on screen */
  .cal-handle.c-nw .cal-label { left:12px; top:12px; }
  .cal-handle.c-ne .cal-label { right:12px; top:12px; }
  .cal-handle.c-se .cal-label { right:12px; bottom:12px; }
  .cal-handle.c-sw .cal-label { left:12px; bottom:12px; }
  .cal-handle.selected .cal-label { background:#0a84ff; }
  #calHelp { position:fixed; left:50%; top:50%; transform:translate(-50%,-50%); z-index:1150; display:none; max-width:440px; background:rgba(17,17,17,0.9); color:#fff; font:13px/1.45 -apple-system, Helvetica, Arial, sans-serif; padding:14px 16px; border-radius:10px; }
  body.calibrating #calHelp { display:block; }
  #calHelp b { color:#9fd0ff; }
  #calHelp input { width:100%; margin-top:6px; font:11px/1.3 ui-monospace, Menlo, monospace; }
  #calHelp button { margin-top:8px; margin-right:6px; font:inherit; cursor:pointer; }
  body.menu-hidden .leaflet-control-layers, body.menu-hidden #heatLoadingBanner { display:none !important; }
  .heat-panel .hp-hide { display:block; width:100%; margin-top:10px; font:inherit; cursor:pointer; }
  .menu-hint { position:fixed; left:50%; bottom:24px; transform:translateX(-50%); z-index:1000; background:rgba(17,17,17,0.85); color:#fff; font:600 13px/1.4 -apple-system, Helvetica, Arial, sans-serif; padding:6px 14px; border-radius:14px; opacity:0; transition:opacity 0.4s; pointer-events:none; }
  .menu-hint.show { opacity:1; }
  /* Readability on the projected model: every panel and label is twice the
     dashboard size. The menu is scaled as a block (it lives outside the
     warped map, so zoom is safe); popups/tooltips inside the map get larger
     fonts instead, since zoom there would throw off Leaflet's positioning. */
  #projControls { zoom:2; max-height:calc(50vh - 10px); overflow-y:auto; }
  .leaflet-popup-content { font-size:26px; margin:20px 30px 20px 24px; }
  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper { border-radius:20px; }
  .leaflet-container a.leaflet-popup-close-button { font-size:32px; width:40px; height:40px; line-height:40px; }
  .glass-tooltip { font-size:24px; padding:12px 18px; border-radius:18px !important; }
  #heatLoadingBanner { font-size:24px; padding:10px 24px; border-radius:24px; }
  .menu-hint { font-size:26px; padding:10px 24px; border-radius:24px; }
</style>
</head>
<body>
  <div id="dashmap"></div>
  <script>
    const PROJECTOR_UI_SCALE = 2;
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

    // ---- Projector geometry + calibration (corner pin) ----
    // The physical model's four corners are PROJECT_BOUNDS' corners. With no
    // manual calibration the box is placed exactly where TransportLab's
    // smart-city rig puts it (params.json5): a full-width box with the
    // model's aspect ratio, vertically centred and nudged down by
    // vertical_offset, the bounds fitted inside it, then stretched
    // horizontally by horizontal_scale about the centre. If that leaves a
    // corner off-screen the box is shrunk (same stretch ratio and centre)
    // until all four are on screen. Everything outside the box is black. In calibration mode (C, or the
    // "Calibrate" button) a crosshair sits on each model corner; drag them
    // (or select one and use the arrow keys, Shift = 10 px) until each lands
    // on the matching corner of the physical model. The map is then warped
    // with a perspective transform so the box's corners map exactly onto the
    // four crosshairs (correcting position, scale, rotation and keystone), and
    // everything outside the quadrilateral they form is solid black, so
    // nothing is projected past the model.
    //
    // Saved per browser (localStorage) as fractions of the window, and
    // shareable as ?cal=... (the help box shows the full link — bookmark it on
    // the projector laptop so a cleared browser can't lose the calibration).
    // Exact at ground level; the model's buildings stand proud of the table,
    // so align to street-level corners, not roof tops.
    // Move the menu out of the map container so the warp doesn't skew it.
    const projControls = document.createElement('div');
    projControls.id = 'projControls';
    document.body.appendChild(projControls);
    projControls.appendChild(layersControl.getContainer());
    // Same for the "Updating heatmap…" banner: inside the map it would be
    // warped and sit in the masked band above the model. Pinned to the
    // screen instead, just inside the model's top edge (applyCalibration).
    document.body.appendChild(heatLoadingBanner);

    const CORNERS = ['NW', 'NE', 'SE', 'SW'];
    // v2: calibrations saved before the smart-city default geometry were
    // relative to the old fill-the-window layout, so they're not reused.
    const CAL_KEY = 'projector.calibration.v2';
    const mapEl = document.getElementById('dashmap');
    let dst = null;        // [[x,y] x4] screen px, NW NE SE SW; null = smart-city default
    let selected = 0;

    const MODEL_ASPECT = {{ model_aspect }};   // model.width / model.height
    const V_OFFSET = {{ v_offset }};           // projector.vertical_offset (px)
    const H_SCALE = {{ h_scale }};             // projector.horizontal_scale

    // Where smart-city's CSS/Leaflet layout puts the model box on screen
    // (NW NE SE SW): #map is 100vw x 100vw/aspect, flex-centred with
    // margin-top: V_OFFSET (so the box moves down V_OFFSET/2), fitBounds()
    // with zoomSnap 0 centres the bounds inside it, and scaleX(H_SCALE)
    // stretches about the container's centre.
    function defaultQuad() {
      const W = window.innerWidth, H = window.innerHeight;
      const Hc = W / MODEL_ASPECT;
      const top = (H - (Hc + V_OFFSET)) / 2 + V_OFFSET;
      const [[sx0, sy0], , [sx1, sy1]] = srcCorners();
      const bw = sx1 - sx0, bh = sy1 - sy0;
      const s = Math.min(W / bw, Hc / bh);
      let w = bw * s * H_SCALE, h = bh * s;
      const cy = top + Hc / 2;  // smart-city's vertical centre of the box
      // At the rig's settings the stretched box is wider than the screen,
      // which would put the east/west corners (and their crosshairs) off the
      // page. Shrink it uniformly, keeping the stretch ratio and the box's
      // centre, until all four corners are on screen.
      const fit = Math.min(1, W / w, H / h, 2 * cy / h, 2 * (H - cy) / h);
      w *= fit; h *= fit;
      const x0 = W / 2 - w / 2, x1 = W / 2 + w / 2;
      const y0 = cy - h / 2, y1 = cy + h / 2;
      return [[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
    }

    // Unwarped pixel corners of the model box (what the warp maps FROM).
    // latLngToContainerPoint() rounds to whole pixels, which is ~1 px off at
    // the projector's scale, so project without rounding.
    const exactPoint = ll => map.layerPointToContainerPoint(map.project(ll).subtract(map.getPixelOrigin()));
    function srcCorners() {
      const nw = exactPoint(PROJECT_BOUNDS.getNorthWest());
      const se = exactPoint(PROJECT_BOUNDS.getSouthEast());
      return [[nw.x, nw.y], [se.x, nw.y], [se.x, se.y], [nw.x, se.y]];
    }

    // Homography H (3x3, h33 = 1) mapping 4 src points to 4 dst points:
    // solve the 8x8 linear system by Gaussian elimination.
    function homography(src, dstPts) {
      const A = [], bv = [];
      for (let i = 0; i < 4; i++) {
        const [x, y] = src[i], [u, v] = dstPts[i];
        A.push([x, y, 1, 0, 0, 0, -x * u, -y * u]); bv.push(u);
        A.push([0, 0, 0, x, y, 1, -x * v, -y * v]); bv.push(v);
      }
      const n = 8;
      for (let c = 0; c < n; c++) {
        let piv = c;
        for (let r = c + 1; r < n; r++) if (Math.abs(A[r][c]) > Math.abs(A[piv][c])) piv = r;
        [A[c], A[piv]] = [A[piv], A[c]]; [bv[c], bv[piv]] = [bv[piv], bv[c]];
        if (Math.abs(A[c][c]) < 1e-12) return null;  // degenerate (e.g. three corners in a line)
        for (let r = 0; r < n; r++) {
          if (r === c) continue;
          const f = A[r][c] / A[c][c];
          if (!f) continue;
          for (let k = c; k < n; k++) A[r][k] -= f * A[c][k];
          bv[r] -= f * bv[c];
        }
      }
      return bv.map((val, i) => val / A[i][i]);  // h11 h12 h13 h21 h22 h23 h31 h32
    }

    const svgNS = 'http://www.w3.org/2000/svg';
    const maskSvg = document.createElementNS(svgNS, 'svg');
    maskSvg.id = 'projMask';
    const maskPath = document.createElementNS(svgNS, 'path');
    maskPath.setAttribute('fill', '#000');
    maskPath.setAttribute('fill-rule', 'evenodd');
    maskSvg.appendChild(maskPath);
    document.body.appendChild(maskSvg);

    function currentQuad() { return dst || defaultQuad(); }

    function applyCalibration() {
      const src = srcCorners();
      let q = currentQuad();
      let h = homography(src, q);
      if (!(h && h.every(Number.isFinite))) {  // degenerate manual quad: fall back
        q = defaultQuad();
        h = homography(src, q);
      }
      if (h && h.every(Number.isFinite)) {
        const [h11, h12, h13, h21, h22, h23, h31, h32] = h;
        mapEl.style.transform = `matrix3d(${h11},${h21},0,${h31},${h12},${h22},0,${h32},0,0,1,0,${h13},${h23},0,1)`;
      } else {
        mapEl.style.transform = '';
      }
      // Black everywhere except inside the quadrilateral.
      const W = window.innerWidth, H = window.innerHeight;
      maskSvg.setAttribute('viewBox', `0 0 ${W} ${H}`);
      maskPath.setAttribute('d', `M0 0H${W}V${H}H0Z M${q.map(p => p.join(' ')).join(' L')} Z`);
      // Banner: centred on the visible part of the model's top edge.
      const topY = Math.max(q[0][1], q[1][1]);
      const midX = (Math.max(0, Math.min(q[0][0], q[3][0])) + Math.min(W, Math.max(q[1][0], q[2][0]))) / 2;
      heatLoadingBanner.style.top = Math.min(Math.max(topY, 0) + 10, H - 40) + 'px';
      heatLoadingBanner.style.left = midX + 'px';
      handles.forEach((hd, i) => {
        hd.style.left = q[i][0] + 'px'; hd.style.top = q[i][1] + 'px';
        hd.classList.toggle('selected', i === selected);
      });
      updateShareLink();
    }

    function saveCalibration() {
      try {
        if (dst) localStorage.setItem(CAL_KEY, JSON.stringify(toFractions(dst)));
        else localStorage.removeItem(CAL_KEY);
      } catch (e) { /* storage unavailable */ }
    }
    const toFractions = pts => pts.map(([x, y]) => [+(x / window.innerWidth).toFixed(5), +(y / window.innerHeight).toFixed(5)]);
    const fromFractions = fr => fr.map(([fx, fy]) => [fx * window.innerWidth, fy * window.innerHeight]);
    function loadCalibration() {
      const param = new URLSearchParams(window.location.search).get('cal');
      let fr = null;
      if (param) {
        const nums = param.split(',').map(Number);
        if (nums.length === 8 && nums.every(Number.isFinite)) fr = [0, 2, 4, 6].map(i => [nums[i], nums[i + 1]]);
      }
      if (!fr) { try { fr = JSON.parse(localStorage.getItem(CAL_KEY) || 'null'); } catch (e) { fr = null; } }
      return Array.isArray(fr) && fr.length === 4 ? fromFractions(fr) : null;
    }

    // Crosshair handles
    const handles = CORNERS.map((name, i) => {
      const hd = document.createElement('div');
      hd.className = `cal-handle c-${name.toLowerCase()}`;
      // The transparent rect makes the whole 80 px square grabbable, not just
      // the drawn lines — the default corners can sit right on the window edge.
      hd.innerHTML = `<svg width="80" height="80"><rect width="80" height="80" fill="transparent" pointer-events="all"/><circle cx="40" cy="40" r="14" fill="none" stroke="#fff" stroke-width="3"/><circle cx="40" cy="40" r="14" fill="none" stroke="#d0021b" stroke-width="1.5"/><path d="M0 40H80M40 0V80" stroke="#fff" stroke-width="3"/><path d="M0 40H80M40 0V80" stroke="#d0021b" stroke-width="1.5"/></svg><span class="cal-label">${name}</span>`;
      document.body.appendChild(hd);
      hd.addEventListener('pointerdown', e => {
        e.preventDefault();
        selected = i;
        if (!dst) dst = defaultQuad();
        const start = [e.clientX, e.clientY], orig = dst[i].slice();
        const move = ev => { dst[i] = [orig[0] + ev.clientX - start[0], orig[1] + ev.clientY - start[1]]; applyCalibration(); };
        const up = () => { window.removeEventListener('pointermove', move); window.removeEventListener('pointerup', up); saveCalibration(); };
        window.addEventListener('pointermove', move);
        window.addEventListener('pointerup', up);
        applyCalibration();
      });
      return hd;
    });

    const calHelp = document.createElement('div');
    calHelp.id = 'calHelp';
    calHelp.innerHTML = `<div><b>Calibration</b>: drag each crosshair onto the matching corner of the model
      (NW = top-left of the map). Tab selects the next crosshair; arrow keys nudge it (Shift = 10 px).
      Align to street-level corners, not building tops.</div>
      <div style="margin-top:6px">Bookmark this link on the projector laptop to restore this calibration:</div>
      <input id="calLink" readonly>
      <div><button type="button" id="calDone">Done (C)</button><button type="button" id="calReset">Reset (R)</button></div>`;
    document.body.appendChild(calHelp);
    const calLink = calHelp.querySelector('#calLink');
    calLink.addEventListener('focus', () => calLink.select());
    function updateShareLink() {
      const url = new URL(window.location.href);
      url.searchParams.delete('cal');
      if (dst) url.searchParams.set('cal', toFractions(dst).flat().join(','));
      calLink.value = url.toString();
    }

    function setCalibrating(on) {
      document.body.classList.toggle('calibrating', on);
      if (on) document.body.classList.remove('menu-hidden');
      applyCalibration();
    }
    calHelp.querySelector('#calDone').addEventListener('click', () => setCalibrating(false));
    calHelp.querySelector('#calReset').addEventListener('click', () => { dst = null; saveCalibration(); applyCalibration(); });

    const calBtn = document.createElement('button');
    calBtn.type = 'button';
    calBtn.className = 'hp-hide';
    calBtn.textContent = 'Calibrate projection (C)';
    calBtn.addEventListener('click', () => setCalibrating(!document.body.classList.contains('calibrating')));
    document.querySelector('.heat-panel').appendChild(calBtn);

    document.addEventListener('keydown', e => {
      if (e.target.closest('input, select, textarea')) return;
      const calibrating = document.body.classList.contains('calibrating');
      if (e.key === 'c' || e.key === 'C') { setCalibrating(!calibrating); e.preventDefault(); return; }
      if (!calibrating) return;
      if (e.key === 'Enter' || e.key === 'Escape') { setCalibrating(false); return; }
      if (e.key === 'r' || e.key === 'R') { dst = null; saveCalibration(); applyCalibration(); return; }
      if (e.key === 'Tab') { selected = (selected + (e.shiftKey ? 3 : 1)) % 4; applyCalibration(); e.preventDefault(); return; }
      const step = e.shiftKey ? 10 : 1;
      const d = { ArrowLeft: [-step, 0], ArrowRight: [step, 0], ArrowUp: [0, -step], ArrowDown: [0, step] }[e.key];
      if (!d) return;
      e.preventDefault();
      if (!dst) dst = defaultQuad();
      dst[selected] = [dst[selected][0] + d[0], dst[selected][1] + d[1]];
      applyCalibration();
      saveCalibration();
    });

    dst = loadCalibration();
    map.on('move zoom resize', applyCalibration);
    window.addEventListener('resize', () => {
      map.invalidateSize();
      dst = loadCalibration();  // stored as window fractions, so it follows a resolution change
      applyCalibration();
    });
    applyCalibration();

    // Hide the map menu so only the map reaches the projector. "Hide menu"
    // button in the menu, M to toggle, ?menu=0 / ?menu=1 in the URL to force
    // it on load; otherwise the last choice is remembered in this browser.
    // The periodic "Updating heatmap…" banner is suppressed while hidden too.
    (function () {
      const STORE_KEY = 'projector.menuHidden';
      const params = new URLSearchParams(window.location.search);
      let hidden;
      if (params.has('menu')) hidden = params.get('menu') === '0';
      else { try { hidden = localStorage.getItem(STORE_KEY) === '1'; } catch (e) { hidden = false; } }
      const hint = document.createElement('div');
      hint.className = 'menu-hint';
      hint.textContent = 'Menu hidden \u2014 press M to show it';
      document.body.appendChild(hint);
      let hintTimer = null;
      function apply(announce) {
        document.body.classList.toggle('menu-hidden', hidden);
        try { localStorage.setItem(STORE_KEY, hidden ? '1' : '0'); } catch (e) { /* storage unavailable */ }
        clearTimeout(hintTimer);
        hint.classList.toggle('show', hidden && announce);
        if (hidden && announce) hintTimer = setTimeout(() => hint.classList.remove('show'), 2500);
      }
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'hp-hide';
      btn.textContent = 'Hide menu (M)';
      btn.addEventListener('click', () => { hidden = true; apply(true); });
      document.querySelector('.heat-panel').appendChild(btn);
      document.addEventListener('keydown', e => {
        if (e.key !== 'm' && e.key !== 'M') return;
        if (e.target.closest('input, select, textarea')) return;
        hidden = !hidden;
        apply(true);
      });
      apply(false);
    })();
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
    model_w, model_h = SMART_CITY_MODEL_MM
    return render_template_string(
        PROJECT_PAGE,
        vehicles=vehicles, vehicles_json=json.dumps(vehicles),
        color_fill=COLOR_FILL,
        ne_lat=ne_lat, ne_lng=ne_lng, sw_lat=sw_lat, sw_lng=sw_lng,
        model_aspect=model_w / model_h,
        v_offset=SMART_CITY_PROJ_VERTICAL_OFFSET_PX,
        h_scale=SMART_CITY_PROJ_HORIZONTAL_SCALE,
    )


# Compress large text responses. The dashboard page is ~1.2 MB of HTML and
# /api/vehicles is polled every 15 s by every open tab; uncompressed, a
# dashboard or projector left open all day sent gigabytes a day, and Render
# suspends Free services that use up their monthly outbound bandwidth.
GZIP_MIN_BYTES = 1024
_GZIP_TYPES = ("text/html", "application/json", "text/csv", "text/plain", "application/javascript")


@app.after_request
def _gzip_response(resp):
    try:
        if (resp.status_code != 200 or resp.direct_passthrough
                or "Content-Encoding" in resp.headers
                or "gzip" not in request.headers.get("Accept-Encoding", "")
                or resp.mimetype not in _GZIP_TYPES):
            return resp
        data = resp.get_data()
        if len(data) < GZIP_MIN_BYTES:
            return resp
        resp.set_data(gzip.compress(data, 5))
        resp.headers["Content-Encoding"] = "gzip"
        resp.headers["Vary"] = "Accept-Encoding"
    except Exception as e:  # never let compression break a response
        print(f"[gzip] skipped: {e}", flush=True)
    return resp


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
        "rss_mb": round(_rss_mb() or 0),
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
        sort_key=data["sort_key"], ascending=data["ascending"],
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


def _fetch_scrape_json(path, timeout=20):
    """GET a JSON file from gtfs-r-scrape's shapes/ folder; None on any failure."""
    url = f"{TRIP_LOOKUP_BASE}/{path}"
    try:
        r = requests.get(url, headers=RAW_HEADERS, timeout=timeout)
        if r.status_code in RETRYABLE_STATUS:
            print(f"[trip] GET {url} -> HTTP {r.status_code}, retrying once", flush=True)
            time.sleep(_retry_after_sec(r))
            r = requests.get(url, headers=RAW_HEADERS, timeout=timeout)
        if r.status_code != 200:
            print(f"[trip] GET {url} -> HTTP {r.status_code}", flush=True)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as e:
        print(f"[trip] GET {url} failed: {e}", flush=True)
        return None


def get_trip_lookup_meta():
    """(n_shards, {stop_id: (lat, lon, name)}), or (None, {}) if the lookup
    hasn't been built yet. A new build (different 'generated' stamp) also
    drops cached shards, since they may now point at different files."""
    now = time.monotonic()
    c = _trip_meta_cache
    if c["n_shards"] and now - c["fetched_at"] < TRIP_LOOKUP_TTL_SECONDS:
        return c["n_shards"], c["stops"]
    with _trip_lookup_lock:
        now = time.monotonic()
        if c["n_shards"] and now - c["fetched_at"] < TRIP_LOOKUP_TTL_SECONDS:
            return c["n_shards"], c["stops"]
        manifest = _fetch_scrape_json("trip_lookup.json")
        if not manifest or not manifest.get("n_shards"):
            return c["n_shards"], c["stops"] or {}  # keep a stale copy if we have one
        stops_raw = _fetch_scrape_json("stops.json", timeout=30) or {}
        stops = {sid: (v[0] / 1e5, v[1] / 1e5, v[2]) for sid, v in stops_raw.items()
                 if isinstance(v, list) and len(v) >= 3}
        if manifest.get("generated") != c["generated"]:
            _trip_shards.clear()
        c.update(n_shards=int(manifest["n_shards"]), generated=manifest.get("generated"),
                 stops=stops or c["stops"] or {}, fetched_at=now)
        print(f"[trip] lookup {c['generated']}: {c['n_shards']} shards, {len(c['stops'])} stops", flush=True)
        return c["n_shards"], c["stops"]


def _get_trip_shard(kind, key, n_shards):
    k = zlib.crc32(key.encode("utf-8")) % n_shards
    ck = (kind, k)
    now = time.monotonic()
    with _trip_lookup_lock:
        hit = _trip_shards.get(ck)
        if hit and now - hit[0] < TRIP_LOOKUP_TTL_SECONDS:
            _trip_shards.move_to_end(ck)
            return hit[1]
    data = _fetch_scrape_json(f"{kind}/{k:03d}.json")
    if data is None:
        return hit[1] if hit else None  # stale copy if we have one
    with _trip_lookup_lock:
        _trip_shards[ck] = (now, data)
        _trip_shards.move_to_end(ck)
        while len(_trip_shards) > TRIP_SHARD_CACHE_MAX:
            _trip_shards.popitem(last=False)
    return data


def compute_trip_detail(trip_id):
    """Route shape and upcoming stops for one live trip.

    Stops: every stop_time_update the live feed currently has for the trip
    (TfNSW only lists stops still to come), in stop_sequence order, each
    with its predicted delay — so the response shows how the bus's current
    lateness is expected to grow or recover along the rest of the route.
    Shape: the timetable shape for the trip from gtfs-r-scrape, encoded as
    in /api/route_shapes; null if the trip isn't in the lookup."""
    all_rows, agency_names, trip_headsigns, _ = get_all_rows_cached()
    n_shards, stops_lookup = get_trip_lookup_meta()

    trip_rows = sorted((r for r in all_rows if r["trip_id"] == trip_id), key=lambda r: r["stop_sequence"])
    stops = []
    for r in trip_rows:
        info = stops_lookup.get(r["stop_id"])
        stops.append({
            "stop_id": r["stop_id"],
            "seq": r["stop_sequence"],
            "name": info[2] if info else None,
            "lat": info[0] if info else None,
            "lon": info[1] if info else None,
            "delay_sec": None if r["anomaly"] else r["delay"],
        })

    shape, shape_error = None, None
    if not n_shards:
        shape_error = "Route lookup not built yet"
    else:
        trips = _get_trip_shard("trips", trip_id, n_shards)
        shape_id = trips.get(trip_id) if trips is not None else None
        if shape_id is None and trips is not None and "_" in trip_id:
            # Operators' added/duplicated runs appear in the live feed as
            # "<timetabled trip>_2" etc. and follow the original's route.
            base = trip_id.rsplit("_", 1)[0]
            base_trips = _get_trip_shard("trips", base, n_shards)
            shape_id = base_trips.get(base) if base_trips else None
        if trips is None:
            shape_error = "Couldn't load route lookup"
        elif shape_id is None:
            shape_error = "Trip not in this week's timetable"
        else:
            geom = _get_trip_shard("geom", shape_id, n_shards)
            shape = geom.get(shape_id) if geom is not None else None
            if shape is None:
                shape_error = "Route shape unavailable"

    route_id = trip_rows[0]["route_id"] if trip_rows else None
    route_num, route_operator = split_route(route_id, agency_names) if route_id else ("", "")
    return {
        "trip_id": trip_id,
        "route_id": route_id,
        "route_num": route_num,
        "route_operator": route_operator,
        "headsign": trip_headsigns.get(trip_id) if trip_headsigns else None,
        "shape": shape,
        "shape_error": shape_error,
        "stops": stops,
    }


@app.route("/api/trip/<path:trip_id>")
def api_trip(trip_id):
    """Route shape and upcoming stops (with predicted delays) for one live
    trip — what the map draws when a bus marker is clicked."""
    if not API_KEY:
        return jsonify({"error": "TFNSW_API_KEY not set in .env"}), 500
    try:
        return jsonify(compute_trip_detail(trip_id))
    except Exception as e:
        return jsonify({"trip_id": trip_id, "shape": None, "stops": [], "error": f"Server error: {e}"}), 200


# Built /api/heatmap bodies, gzipped once and reused until the window is
# rebuilt: key (window, metric, period, build time) -> (etag, gz bytes).
# Every open dashboard re-polls every 5 minutes, but a window only changes
# every 5-30 minutes, so most polls are answered 304 Not Modified (no body)
# and the rest get a pre-compressed body without recomputing anything.
_heatmap_body_cache = OrderedDict()
_heatmap_body_lock = threading.Lock()
HEATMAP_BODY_CACHE_MAX = 24


def _gzip_json_response(gz, etag=None):
    if "gzip" in request.headers.get("Accept-Encoding", ""):
        resp = Response(gz, mimetype="application/json")
        resp.headers["Content-Encoding"] = "gzip"
    else:
        resp = Response(gzip.decompress(gz), mimetype="application/json")
    resp.headers["Vary"] = "Accept-Encoding"
    resp.direct_passthrough = False
    if etag:
        resp.headers["ETag"] = f'"{etag}"'
        # Browser keeps the body and revalidates every time: 304 when unchanged.
        resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/heatmap")
def api_heatmap():
    try:
        window, werr = parse_window_args(request.args)
        if werr:
            return jsonify({"points": [], "window_hours": None, "metric": None, "period": None, "error": werr})
        window_hours = window.hours
        metric = request.args.get("metric", "delay")
        if metric not in VALID_HEATMAP_METRICS:
            metric = "delay"
        period = request.args.get("period", "all")
        if period not in VALID_HEATMAP_PERIODS:
            period = "all"

        meta = get_historical_cells_cached(window)
        body_key = (window.key, metric, period, meta.get("version"), meta.get("last_error"))
        with _heatmap_body_lock:
            hit = _heatmap_body_cache.get(body_key)
            if hit is not None:
                _heatmap_body_cache.move_to_end(body_key)
        if hit is None:
            points, error, prior = get_heatmap_points_cached(window, metric, period)
            body = json.dumps({"points": points, "prior": prior, "window_hours": window_hours,
                               "window": str(window.key), "metric": metric, "period": period,
                               "data_end": meta.get("data_end"), "error": error},
                              separators=(",", ":")).encode()
            etag = f"{zlib.crc32(repr(body_key).encode()):08x}-{len(body)}"
            hit = (etag, gzip.compress(body, 6))
            del body, points
            _trim_memory()
            _log_rss(f"heatmap body {window.key}/{metric}/{period}")
            with _heatmap_body_lock:
                _heatmap_body_cache[body_key] = hit
                while len(_heatmap_body_cache) > HEATMAP_BODY_CACHE_MAX:
                    _heatmap_body_cache.popitem(last=False)
        etag, gz = hit
        if request.if_none_match.contains(etag):
            resp = Response(status=304)
            resp.headers["ETag"] = f'"{etag}"'
            resp.headers["Cache-Control"] = "no-cache"
            return resp
        return _gzip_json_response(gz, etag)
    except Exception as e:
        return jsonify({"points": [], "window_hours": None, "metric": None, "period": None,
                        "error": f"Server error: {e}"}), 200


@app.route("/api/validation")
def api_validation():
    """On-time running by operator from the scraped history, for comparing
    against TfNSW's published monthly on-time running results.

    ?window=1|24|168 or ?window=custom&start=YYYY-MM-DD&end=YYYY-MM-DD;
    &format=csv for a spreadsheet-ready table. Operators are grouped by NAME
    (several agency ids can share one, e.g. multiple Transit Systems
    contracts), using the timetable's agency.txt.

    Not like-for-like with the published KPI, and the response says so:
    each reading is one bus at one scrape snapshot (delay at its latest
    reported stop, mid-trip), whereas the published figures measure
    timetabled trips at defined points. Mid-trip delay accumulates along a
    route, so these figures should read somewhat LOWER than published."""
    window, werr = parse_window_args(request.args)
    if werr:
        return jsonify({"error": werr}), 400
    meta = get_historical_cells_cached(window)
    agency_names, _, agency_error = get_schedule_lookups()

    by_name = {}
    for agency_id, (n, early, on, late, late_sum) in meta.get("agency_stats", {}).items():
        name = agency_names.get(agency_id, f"Unknown operator ({agency_id})")
        g = by_name.setdefault(name, {"operator": name, "agency_ids": [], "n": 0, "early": 0,
                                      "on_time": 0, "late": 0, "late_sum": 0.0})
        g["agency_ids"].append(agency_id)
        g["n"] += n; g["early"] += early; g["on_time"] += on; g["late"] += late; g["late_sum"] += late_sum

    def finish(g):
        n = g["n"] or 1
        return {"operator": g["operator"], "agency_ids": sorted(g["agency_ids"]), "readings": g["n"],
                "on_time_pct": round(100 * g["on_time"] / n, 1),
                "early_pct": round(100 * g["early"] / n, 1),
                "late_pct": round(100 * g["late"] / n, 1),
                "mean_lateness_min": round(g["late_sum"] / n / 60, 2)}

    rows = sorted((finish(g) for g in by_name.values()), key=lambda r: -r["readings"])
    total = {"operator": "All operators", "agency_ids": [], "n": 0, "early": 0, "on_time": 0, "late": 0, "late_sum": 0.0}
    for g in by_name.values():
        for k in ("n", "early", "on_time", "late", "late_sum"):
            total[k] += g[k]
    network = finish(total)

    if request.args.get("format") == "csv":
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["operator", "agency_ids", "readings", "on_time_pct", "early_pct", "late_pct", "mean_lateness_min"])
        for r in rows + [network]:
            w.writerow([r["operator"], " ".join(r["agency_ids"]), r["readings"], r["on_time_pct"],
                        r["early_pct"], r["late_pct"], r["mean_lateness_min"]])
        return Response(out.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=otr_{str(window.key).replace('..', '_to_')}.csv"})

    return jsonify({
        "window": window.describe(), "files_fetched": meta["files_fetched"],
        "deadline_hit": meta["deadline_hit"], "operator_names_error": agency_error,
        "on_time_definition": "0:59 early to 5:59 late inclusive (ON_TIME_EARLY_SEC..ON_TIME_LATE_SEC)",
        "method_note": ("One reading per bus per scrape snapshot, using the delay at its latest reported stop "
                        "(mid-trip). Published TfNSW on-time running measures timetabled trips at defined "
                        "points, so expect these figures to read somewhat lower; compare ranking and spread, "
                        "not exact values."),
        "network": network, "operators": rows,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
