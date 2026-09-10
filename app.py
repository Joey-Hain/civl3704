"""
Web dashboard for TfNSW GTFS-realtime delay/variance data.

Fetches the live trip-update feed on each page load, computes delay stats
per operator, route and trip, and renders a sortable HTML table. Also
appends every pull to delay_log.csv so you build up history.

A live map sits at the top of the page, fed by the separate GTFS-realtime
VEHICLE POSITION feed (hardcoded to the vehiclepos endpoint below — this is
intentionally NOT read from TFNSW_GTFS_RT_URL in .env, since that variable
is dedicated to the trip-update feed the delay board depends on, and the
two products are subscribed to separately on the TfNSW developer portal).
Vehicle positions are joined to trip-update delay readings by trip_id, so
each bus marker's colour and popup reflect its current delay. The map
polls /api/vehicles every 15s independently of the (page-load-only) tables
below, and respects whatever route/stop/operator/hide_anomalies filters are
currently set.

LIQUID-GLASS POPUPS: purely a frontend CSS concern.

COLOUR SCHEME: every bus marker has the same blue fill, with delay status
shown via the marker's OUTLINE colour.

DENSITY HEATMAP (live): a toggleable layer (via leaflet.heat) showing where
live buses are currently clustered.

DENSITY HEATMAP (historical): a second toggleable layer, pulling position
data from the gtfs-r-scrape repo.

HEATMAP ZOOM-LOCK: both heat layers size their radius/blur in METRES,
converted to screen pixels on every zoom change so the apparent geographic
scale stays constant. Because pure metre-based sizing collapses to invisible
sub-pixel dots at low zoom (220m ≈ 3px at a whole-Sydney view), the pixel
radius is FLOORED at HEAT_MIN_RADIUS_PX. Below the crossover zoom (~14 in
Sydney), blobs sit at a fixed pixel size so dense corridors stay readable.
Above it, metre-based sizing takes over. Both layers share the same sizing
so live and historical look identical at every zoom level.

TRIP HEADSIGNS: enabled. Set ENABLE_TRIP_HEADSIGNS=0 in Render's env to
disable (frees ~60-80MB on the free tier).

=== MEMORY: WHY THIS FILE IS SHAPED THE WAY IT IS ===

The Render free tier gives 512MB RAM. Three distinct things have historically
blown it. All three are now fixed, and none of them are obvious from reading
any single function — the fixes are spread across the file.

  1. Schedule bundle DOWNLOAD. Fixed by streaming the schedule zip to disk
     instead of resp.content. See _download_to_path().

  2. Schedule bundle REPARSE ON EVERY REQUEST. This was the sneaky one. The
     row cache has a 12s TTL, and get_all_rows_cached() called
     load_schedule_lookups() on every expiry. That function read the 60-
     150MB agency_names.json off disk and re-built the dicts, every 12
     seconds, while the previous dicts were still referenced by the row
     cache. That's tens-of-MB churn per tick — enough on a 512MB instance
     to coincide with a heatmap fetch and get the worker OOM-killed.
     Fixed by caching the parsed schedule IN MEMORY (_schedule_cache) so it
     is loaded at most ONCE per process. The disk JSON is now only read on
     cold start. See load_schedule_lookups().

  3. Historical heatmap fetch. Fixed by streaming each daily CSV line-by-
     line via requests(stream=True) + iter_lines + csv.reader, aggregating
     rows into a local grid dict as they arrive. Peak RSS per worker is one
     line, not the whole file. See _fetch_one_day_into().

Additionally:

  * A global _heatmap_fetch_lock ensures only one historical fetch runs at
    a time. Without it, quickly switching the window dropdown (1h → 24h →
    168h) could stack up 3 concurrent multi-file fetches, each opening 2
    files, which was another way to reach the memory ceiling.

  * A wall-clock deadline (HEATMAP_DEADLINE_SEC) caps the multi-day fetch:
    partial data with a note is infinitely better than a truncated JSON
    body.

  * Only ONE gunicorn worker (each worker is a separate process with its
    own copy of every cache). Threads are the right concurrency tool — the
    workload is HTTP-bound, not CPU-bound.

=== RENDER SETTINGS THAT MUST BE SET ===

  Start Command (single line, no backslashes):
      gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --worker-class gthread --timeout 120 --no-control-socket

  Health Check Path: /ping

  The --no-control-socket flag is not optional.

=== IF IT STILL OOMs ===

  Set ENABLE_TRIP_HEADSIGNS=0 in Render's env vars. Popups show "Route 601"
  instead of "Route 601 to Bondi Beach". After that, the remaining lever is
  a bigger Render instance — 512MB is genuinely tight for a statewide GTFS
  schedule bundle plus live caches.

NOTE: an earlier version of this file also drew GTFS route-shape polylines
under the bus markers. That feature has been removed (scope cut).

Setup:
    pip install flask requests python-dotenv gtfs-realtime-bindings tzdata

.env file:
    TFNSW_API_KEY=your_api_key_here
    TFNSW_GTFS_RT_URL=https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses

Usage:
    python app.py
    then open http://localhost:5000
"""

import codecs
import csv
import io
import json
import os
import shutil
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
AGENCY_CACHE_FILE = DATA_DIR / "agency_names.json"
AGENCY_CACHE_MAX_AGE = timedelta(hours=24)
ANOMALY_ABS_SEC = 3600
ON_TIME_EARLY_SEC = -60
ON_TIME_LATE_SEC = 300

GRID_DECIMALS = 4

# Historical heatmap fetch tuning. Concurrency=2 (was 3): each worker opens
# one CSV and streams it line-by-line, but 3 parallel streams plus the rest
# of the app was occasionally tight on 512MB. 2 is plenty fast and leaves
# headroom.
HEATMAP_FETCH_CONCURRENCY = 2
HEATMAP_DEADLINE_SEC = 45

load_dotenv()
API_KEY = os.getenv("TFNSW_API_KEY")
FEED_URL = os.getenv("TFNSW_GTFS_RT_URL", "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses")
SCHEDULE_URL = os.getenv("TFNSW_GTFS_SCHEDULE_URL", "https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses")

VEHICLE_POS_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"

SCRAPE_REPO = "Joey-Hain/gtfs-r-scrape"
SCRAPE_RAW_BASE = f"https://raw.githubusercontent.com/{SCRAPE_REPO}/main/data"
HEATMAP_WINDOW_CACHE_TTL_SECONDS = 300

COLOR_FILL = "#00B3F0"
OUTLINE_ON_TIME = "#ffffff"
OUTLINE_LATE = "#B3261E"
OUTLINE_EARLY = "#1E6B3C"
OUTLINE_NO_DATA = "#888888"

SYDNEY_CBD = (-33.8688, 151.2093)
SYDNEY_RADIUS_KM = 10

ENABLE_TRIP_HEADSIGNS = os.getenv("ENABLE_TRIP_HEADSIGNS", "1") == "1"


def _log_rss(tag):
    """Log current resident set size to stdout so Render's log stream shows
    exactly where memory goes. No-op on non-Linux."""
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
_heatmap_fetch_lock = threading.Lock()

# IN-MEMORY cache of the parsed schedule. This is the single most important
# memory fix in the file. Prior to this, load_schedule_lookups() re-read and
# re-parsed the 60-150MB agency_names.json off disk every time the row cache
# (12s TTL) expired, while the old dicts were still referenced by the row
# cache. On a 512MB instance that churn was the intermittent OOM trigger.
#
# Now: the schedule is parsed at most ONCE per process (on cold start), and
# every subsequent call returns the same object references. Disk JSON is a
# cold-start fallback only.
_schedule_cache = {
    "agency_names": None,
    "trip_headsigns": None,
    "error": None,
}


def _download_to_path(url, headers, dest_path, timeout=60):
    """Stream a URL to a local file, returning bytes written."""
    total = 0
    with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:
        if r.status_code != 200:
            raise RuntimeError(
                f"Schedule endpoint returned HTTP {r.status_code}. "
                f"This usually means the API key isn't subscribed to the bus schedule/timetable "
                f"product (separate from GTFS Realtime) on the TfNSW developer portal."
            )
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=256 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
    return total


def _parse_csv_member(zf, member, key_col, val_col):
    """Stream-parse one CSV member of an open ZipFile into {key: value}."""
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
        for row in reader:
            if len(row) > max(key_idx, val_idx) and row[val_idx].strip():
                result[row[key_idx].strip()] = row[val_idx].strip()
    return result


def load_schedule_lookups():
    """Return ({agency_id: agency_name}, {trip_id: trip_headsign}, error).

    Returns the SAME in-memory dicts on every call after the first successful
    load. Never re-reads or re-parses the JSON cache file within a process
    lifetime. See the module docstring: repeated re-parsing was the primary
    cause of the intermittent OOM on the free tier.
    """
    with _schedule_lock:
        # Fast path: already loaded in this process. This is what every call
        # after the first one hits, and it's the whole point of the fix.
        if _schedule_cache["agency_names"] is not None:
            return (_schedule_cache["agency_names"],
                    _schedule_cache["trip_headsigns"],
                    _schedule_cache["error"])

        # Cold start: read the disk cache if it exists and is fresh. This
        # runs at most once per process lifetime.
        if AGENCY_CACHE_FILE.exists():
            try:
                with open(AGENCY_CACHE_FILE) as f:
                    cached = json.load(f)
                fetched_at = datetime.fromisoformat(cached["fetched_at"])
                if datetime.now(tz=SYDNEY_TZ) - fetched_at < AGENCY_CACHE_MAX_AGE:
                    _schedule_cache["agency_names"] = cached["agencies"]
                    _schedule_cache["trip_headsigns"] = cached.get("trip_headsigns", {})
                    _schedule_cache["error"] = None
                    _log_rss("schedule-loaded-from-disk")
                    return cached["agencies"], cached.get("trip_headsigns", {}), None
            except Exception:
                pass

        # Cache miss / stale: download from TfNSW and rebuild.
        tmpdir = tempfile.mkdtemp(prefix="tfnsw_schedule_")
        try:
            outer_path = os.path.join(tmpdir, "schedule.zip")
            outer_size = _download_to_path(
                SCHEDULE_URL,
                {"Authorization": f"apikey {API_KEY}"},
                outer_path,
            )
            print(f"[schedule] downloaded {outer_size / 1e6:.1f}MB to disk", flush=True)
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

            print(f"[schedule] parsed {len(agencies)} agencies, {len(trip_headsigns)} trip headsigns", flush=True)
            _log_rss("after-parse")

            if not agencies:
                raise RuntimeError("Downloaded schedule bundle but found no agency.txt / no agency rows in it.")

            with open(AGENCY_CACHE_FILE, "w") as f:
                json.dump({
                    "fetched_at": datetime.now(tz=SYDNEY_TZ).isoformat(),
                    "agencies": agencies,
                    "trip_headsigns": trip_headsigns,
                }, f)

            _schedule_cache["agency_names"] = agencies
            _schedule_cache["trip_headsigns"] = trip_headsigns
            _schedule_cache["error"] = None
            _log_rss("after-cache-write")
            return agencies, trip_headsigns, None
        except Exception as e:
            # If the download failed but a stale disk cache exists, use it.
            if AGENCY_CACHE_FILE.exists():
                try:
                    with open(AGENCY_CACHE_FILE) as f:
                        cached = json.load(f)
                    _schedule_cache["agency_names"] = cached["agencies"]
                    _schedule_cache["trip_headsigns"] = cached.get("trip_headsigns", {})
                    _schedule_cache["error"] = f"Using stale cached names ({e})"
                    return cached["agencies"], cached.get("trip_headsigns", {}), _schedule_cache["error"]
                except Exception:
                    pass
            _schedule_cache["agency_names"] = {}
            _schedule_cache["trip_headsigns"] = {}
            _schedule_cache["error"] = str(e)
            return {}, {}, str(e)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


def fetch_feed():
    headers = {"Authorization": f"apikey {API_KEY}"}
    response = requests.get(FEED_URL, headers=headers, timeout=15)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def fetch_vehicle_feed():
    headers = {"Authorization": f"apikey {API_KEY}"}
    response = requests.get(VEHICLE_POS_URL, headers=headers, timeout=15)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
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
_heatmap_cache = {}


def get_all_rows_cached():
    now = datetime.now(tz=SYDNEY_TZ)
    cached_at = _rows_cache["fetched_at"]
    if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
        return (_rows_cache["all_rows"], _rows_cache["agency_names"], _rows_cache["trip_headsigns"],
                _rows_cache["agency_error"])

    agency_names, trip_headsigns, agency_error = load_schedule_lookups()
    feed = fetch_feed()
    all_rows = extract_rows(feed, agency_names)
    append_to_log(all_rows)

    _rows_cache.update(all_rows=all_rows, agency_names=agency_names, trip_headsigns=trip_headsigns,
                       agency_error=agency_error, fetched_at=now)
    return all_rows, agency_names, trip_headsigns, agency_error


def get_vehicles_cached(agency_names, trip_headsigns):
    now = datetime.now(tz=SYDNEY_TZ)
    cached_at = _vehicles_cache["fetched_at"]
    if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
        return [dict(v) for v in _vehicles_cache["vehicles"]]

    vfeed = fetch_vehicle_feed()
    vehicles = extract_vehicles(vfeed, agency_names, trip_headsigns)
    _vehicles_cache.update(vehicles=vehicles, fetched_at=now)
    return [dict(v) for v in vehicles]


def _fetch_one_day_into(date_str, cutoff, local_cells):
    """Stream one day's CSV and aggregate qualifying rows into local_cells.

    The whole CSV is never held in memory. requests(stream=True) +
    iter_lines + csv.reader parses line-by-line as bytes arrive off the
    socket, and each row is folded into the grid dict before the next line
    is read. Peak RSS per worker is one line, not the whole file.
    """
    url = f"{SCRAPE_RAW_BASE}/{date_str}.csv"
    rows_seen = 0
    points_added = 0
    try:
        with requests.get(url, timeout=60, stream=True) as resp:
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
                return date_str, 0, 0, f"{date_str}: CSV missing required columns (need timestamp, lat, lon)"

            max_idx = max(ts_idx, lat_idx, lon_idx)
            for row in csv.reader(lines):
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
                points_added += 1
            return date_str, rows_seen, points_added, None
    except requests.RequestException as e:
        print(f"[heatmap] GET {url} -> FAILED: {e}", flush=True)
        return date_str, 0, 0, f"{date_str}: {e}"


def fetch_historical_heatmap_points(window_hours):
    """Fetch density points from the gtfs-r-scrape repo's daily CSVs.

    Serialised by _heatmap_fetch_lock: only one historical fetch runs at a
    time across all threads. Without this, rapidly switching the window
    dropdown (1h → 24h → 168h) could stack up 3 concurrent multi-file
    fetches, which was another way to reach the memory ceiling.
    """
    start = time.monotonic()
    now = datetime.now(tz=SYDNEY_TZ)
    cutoff = now - timedelta(hours=window_hours)

    dates_needed = []
    d = cutoff.date()
    while d <= now.date():
        dates_needed.append(d.isoformat())
        d += timedelta(days=1)

    workers = max(1, min(HEATMAP_FETCH_CONCURRENCY, len(dates_needed)))
    local_dicts = [defaultdict(lambda: [0.0, 0.0, 0]) for _ in range(workers)]

    files_fetched = 0
    rows_seen_total = 0
    points_added_total = 0
    last_error = None
    deadline_hit = False

    with _heatmap_fetch_lock:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(_fetch_one_day_into, ds, cutoff, local_dicts[i % workers])
                for i, ds in enumerate(dates_needed)
            ]
            for future in as_completed(futures):
                if time.monotonic() - start > HEATMAP_DEADLINE_SEC:
                    deadline_hit = True
                    print(f"[heatmap] deadline {HEATMAP_DEADLINE_SEC}s hit; "
                          f"returning partial results from {files_fetched} file(s)", flush=True)
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

    cells = defaultdict(lambda: [0.0, 0.0, 0])
    for ld in local_dicts:
        for key, c in ld.items():
            tgt = cells[key]
            tgt[0] += c[0]
            tgt[1] += c[1]
            tgt[2] += c[2]

    print(f"[heatmap] window={window_hours}h files={files_fetched} "
          f"rows={rows_seen_total} points={points_added_total} cells={len(cells)} "
          f"elapsed={time.monotonic() - start:.1f}s deadline_hit={deadline_hit}", flush=True)

    if not cells:
        if files_fetched == 0:
            return [], last_error or "No data files found for this window on GitHub"
        return [], (f"Fetched {files_fetched} file(s) and read {rows_seen_total} rows, but none fell "
                    f"inside the last {window_hours}h — check the 'timestamp' column name and format")

    max_count = max(c[2] for c in cells.values())
    points = []
    for c in cells.values():
        points.append([c[0] / c[2], c[1] / c[2], c[2] / max_count])

    note = None
    if deadline_hit:
        note = (f"Partial data: fetch exceeded {HEATMAP_DEADLINE_SEC}s. "
                f"Showing {files_fetched} of {len(dates_needed)} day(s).")
    return points, note


def get_heatmap_points_cached(window_hours):
    now = datetime.now(tz=SYDNEY_TZ)
    cached = _heatmap_cache.get(window_hours)
    if cached is not None and (now - cached["fetched_at"]).total_seconds() < HEATMAP_WINDOW_CACHE_TTL_SECONDS:
        return cached["points"], cached["error"]

    points, error = fetch_historical_heatmap_points(window_hours)
    _heatmap_cache[window_hours] = {"points": points, "error": error, "fetched_at": now}
    return points, error


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
        "hide_anomalies": hide_anomalies,
        "sort_key": sort_key,
        "ascending": ascending,
        "q_route": q_route,
        "q_stop": q_stop,
        "q_operator": q_operator,
        "agency_names": agency_names,
        "trip_headsigns": trip_headsigns,
        "agency_error": agency_error,
        "all_rows": all_rows,
        "latest_by_trip": latest_by_trip,
        "latest_rows": latest_rows,
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
        allowed_trip_ids = set(data["latest_by_trip"].keys())
        vehicles = [v for v in vehicles if v["trip_id"] in allowed_trip_ids]

    if data["apply_bounds"]:
        lat0, lon0 = SYDNEY_CBD
        vehicles = [
            v for v in vehicles
            if v["lat"] is not None and v["lon"] is not None
            and haversine_km(lat0, lon0, v["lat"], v["lon"]) <= SYDNEY_RADIUS_KM
        ]

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
  :root {
    --bg: #f7f6f2;
    --text: #111111;
    --muted: #666666;
    --line: #cccccc;
    --late: #b3261e;
    --early: #1e6b3c;
    --fill-blue: {{ color_fill }};
    --sans: Helvetica, Arial, sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    margin: 0;
    padding: 24px 32px 60px;
  }
  h1 {
    font-size: 1.4rem;
    font-weight: bold;
    border-bottom: 2px solid var(--text);
    padding-bottom: 10px;
    margin-bottom: 4px;
  }
  .meta { color: var(--muted); font-size: 0.85rem; margin-bottom: 24px; }
  .meta a { color: var(--text); }
  h2 {
    font-size: 1rem;
    font-weight: bold;
    margin-top: 36px;
    border-bottom: 1px solid var(--line);
    padding-bottom: 4px;
  }
  table { border-collapse: collapse; width: 100%; margin-top: 10px; }
  th, td { padding: 6px 12px; text-align: right; }
  th:first-child, td:first-child { text-align: left; }
  th {
    color: var(--muted);
    font-weight: normal;
    font-size: 0.8rem;
    border-bottom: 1px solid var(--line);
  }
  th a { color: inherit; text-decoration: underline; }
  th a:hover { color: var(--late); }
  tr:hover { background: #eeece5; }
  td.late { color: var(--late); }
  td.early { color: var(--early); }
  .flag { color: var(--muted); font-size: 0.75rem; }
  .toggle { color: var(--text); text-decoration: underline; font-size: 0.85rem; }

  #dashmap { height: 600px; border: 1px solid var(--line); margin-top: 10px; background: #e5e3dc; }
  .map-legend {
    display: flex; gap: 16px; align-items: center;
    font-size: 0.8rem; color: var(--muted); margin-top: 8px; flex-wrap: wrap;
  }
  .map-legend .swatch {
    display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    margin-right: 4px; vertical-align: middle;
    background: var(--fill-blue);
    border: 2px solid #999;
  }
  .map-error { color: var(--late); font-size: 0.85rem; margin-top: 8px; }

  .bus-marker { position: relative; width: 56px; height: 24px; }
  .bus-pill {
    position: absolute;
    top: 0; left: 50%;
    transform: translateX(-50%);
    display: flex;
    align-items: center;
    gap: 4px;
    background: var(--fill-blue);
    color: #fff;
    font: 600 11px/1 -apple-system, Helvetica, Arial, sans-serif;
    padding: 5px 7px;
    border-radius: 7px;
    border: 2.5px solid #888;
    box-shadow: 0 1px 3px rgba(0,0,0,0.4);
    white-space: nowrap;
  }
  .bus-arrow {
    flex: 0 0 auto;
    font-size: 12px;
    line-height: 1;
    display: inline-block;
    color: #fff;
  }
  .leaflet-popup-content { font: 13px/1.4 -apple-system, Helvetica, Arial, sans-serif; }

  .glass-tooltip {
    background: rgba(255, 255, 255, 0.55) !important;
    -webkit-backdrop-filter: blur(14px) saturate(180%);
    backdrop-filter: blur(14px) saturate(180%);
    border: 1px solid rgba(255, 255, 255, 0.45) !important;
    border-radius: 12px !important;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.18);
    color: #111;
    font: 600 12px/1.4 -apple-system, Helvetica, Arial, sans-serif;
    padding: 7px 11px;
  }
  .glass-tooltip::before { display: none; }

  .leaflet-popup.glass-popup .leaflet-popup-content-wrapper {
    background: rgba(255, 255, 255, 0.55);
    -webkit-backdrop-filter: blur(14px) saturate(180%);
    backdrop-filter: blur(14px) saturate(180%);
    border: 1px solid rgba(255, 255, 255, 0.45);
    border-radius: 12px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.18);
    color: #111;
  }
  .leaflet-popup.glass-popup .leaflet-popup-tip {
    background: rgba(255, 255, 255, 0.55);
    box-shadow: none;
  }

  .leaflet-control-layers {
    font: 13px/1.4 -apple-system, Helvetica, Arial, sans-serif !important;
  }

  #histWindowPicker {
    font: 12px/1.4 -apple-system, Helvetica, Arial, sans-serif;
    margin: 4px 0 2px 22px;
  }
  #histWindowPicker select { font: inherit; }
  #histWindowStatus { font: 11px/1.4 -apple-system, Helvetica, Arial, sans-serif; color: #b3261e; margin: 2px 0 2px 22px; max-width: 220px; }
</style>
</head>
<body>
  <h1>Delay Board</h1>
  <div class="meta">
    Pulled {{ pulled_at }} &middot; {{ n_total }} readings ({{ n_flagged }} flagged as anomalous, {{ 'hidden' if hide_anomalies else 'shown' }})
    &middot; <a class="toggle" href="?hide_anomalies={{ 0 if hide_anomalies else 1 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">{{ 'show anomalies' if hide_anomalies else 'hide anomalies' }}</a>
    <br>"On time" = arrived no more than 1 min early or 5 min late (standard transit industry window).
    <br>n = number of distinct buses (trips) currently reporting, non-anomalous, taken from each bus's most recent stop.
    {% if agency_error %}<br><span style="color:#b3261e">Operator names unavailable: {{ agency_error }}</span>{% endif %}
    <br><span style="color:#666">{{ agency_debug }}</span>
  </div>

  <form method="get" style="margin: 20px 0; padding: 14px; border: 1px solid var(--line);">
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
    <span>{{ vehicles|length }} vehicles shown{% if filters_active %} (filtered to match route/stop/operator above){% endif %}{% if apply_bounds %} &middot; <a class="toggle" href="?bounds=0&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">within 10km of CBD, show statewide</a>{% else %} &middot; <a class="toggle" href="?bounds=1&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">statewide, restrict to 10km of CBD</a>{% endif %}</span>
    <span>Use the layer switcher (top-right of the map) to toggle the density heatmaps.</span>
  </div>
  {% if map_error %}<div class="map-error">Vehicle positions unavailable: {{ map_error }}</div>{% endif %}

  <h2>By operator</h2>
  <table>
    <tr><th>Operator</th><th>n</th><th>avg delay</th><th>spread (&plusmn;min)</th><th>on time</th><th>range</th></tr>
    {% for r in operators %}
    <tr>
      <td class="route">{{ r.operator }}</td>
      <td>{{ r.n }}</td>
      <td class="{{ 'late' if r.mean_min > 0 else 'early' }}">{{ '%+.1f'|format(r.mean_min) }} min</td>
      <td>{{ '%.1f'|format(r.stdev_min) }} min</td>
      <td>{{ '%.0f'|format(r.on_time_pct) }}%</td>
      <td>{{ '%+.1f'|format(r.min_min) }} to {{ '%+.1f'|format(r.max_min) }} min</td>
    </tr>
    {% endfor %}
  </table>

  <h2>By route &mdash; worst variance first (sort: <a class="toggle" href="?sort=stdev_min&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">spread</a> / <a class="toggle" href="?sort=mean_min&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">avg delay</a> / <a class="toggle" href="?sort=on_time_pct&amp;asc=1&amp;hide_anomalies={{ 1 if hide_anomalies else 0 }}&amp;route={{ q_route }}&amp;stop={{ q_stop }}&amp;operator={{ q_operator }}">worst on-time %</a>)</h2>
  <table>
    <tr><th>Route</th><th>Operator</th><th>n</th><th>avg delay</th><th>spread (&plusmn;min)</th><th>on time</th><th>range</th></tr>
    {% for r in routes[:60] %}
    <tr>
      <td class="route">{{ r.route_num }}</td>
      <td>{{ r.route_operator }}</td>
      <td>{{ r.n }}</td>
      <td class="{{ 'late' if r.mean_min > 0 else 'early' }}">{{ '%+.1f'|format(r.mean_min) }} min</td>
      <td>{{ '%.1f'|format(r.stdev_min) }} min</td>
      <td>{{ '%.0f'|format(r.on_time_pct) }}%</td>
      <td>{{ '%+.1f'|format(r.min_min) }} to {{ '%+.1f'|format(r.max_min) }} min</td>
    </tr>
    {% endfor %}
  </table>

  <h2>Individual trips &mdash; largest single delays (most recent reading per bus)</h2>
  <table>
    <tr><th>Trip</th><th>Route</th><th>Operator</th><th>Most recent stop</th><th>delay</th></tr>
    {% for r in worst_trips[:30] %}
    <tr>
      <td class="route">{{ r.trip_id }}</td>
      <td>{{ r.route_num }}</td>
      <td>{{ r.route_operator }}</td>
      <td>{{ r.stop_id }}</td>
      <td class="{{ 'late' if r.delay > 0 else 'early' }}">{{ '%+.1f'|format(r.delay / 60) }} min{% if r.anomaly %} <span class="flag">(flagged)</span>{% endif %}</td>
    </tr>
    {% endfor %}
  </table>

  <script>
    const map = L.map('dashmap').setView([-33.8688, 151.2093], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    const markers = new Map();

    const HEAT_GRADIENT = { 0.0: '#0d0887', 0.3: '#7e03a8', 0.55: '#cc4778', 0.75: '#f89441', 1.0: '#f0f921' };

    // --- Zoom-locked heat sizing -----------------------------------------
    //
    // Both heat layers size their radius/blur in METRES, converted to
    // screen pixels at the current zoom, so at high zoom a blob represents
    // a fixed geographic area (~220m diameter) regardless of zoom level.
    //
    // But pure metre-based sizing breaks at LOW zoom: at a whole-Sydney
    // view, 220m is only ~3px on screen, so every cell collapses into
    // pixel noise and you lose all structure. The fix is a FLOOR on the
    // computed pixel radius: below the crossover zoom, blobs stay at a
    // fixed pixel size (HEAT_MIN_RADIUS_PX) so dense corridors remain
    // visible. Above the crossover zoom, metre-based sizing takes over.
    //
    // Tune HEAT_MIN_RADIUS_PX up for chunkier blobs when zoomed out, down
    // for finer structure. HEAT_MIN_BLUR_PX controls edge softness at low
    // zoom. Both layers share the same sizing so live and historical look
    // identical at every zoom level.
    const HEAT_RADIUS_M = 220, HEAT_BLUR_M = 200;
    const HEAT_MIN_RADIUS_PX = 12, HEAT_MIN_BLUR_PX = 10;

    function metresToPixels(metres, zoom, lat) {
      const metresPerPixel = 156543.03392 * Math.cos(lat * Math.PI / 180) / Math.pow(2, zoom);
      return metres / metresPerPixel;
    }

    const markersLayer = L.layerGroup().addTo(map);
    const heatLayer = L.heatLayer([], { radius: HEAT_MIN_RADIUS_PX, blur: HEAT_MIN_BLUR_PX, maxZoom: 15, minOpacity: 0.25, gradient: HEAT_GRADIENT });
    const histHeatLayer = L.heatLayer([], { radius: HEAT_MIN_RADIUS_PX, blur: HEAT_MIN_BLUR_PX, maxZoom: 14, minOpacity: 0.25, gradient: HEAT_GRADIENT });

    function updateHeatRadii() {
      const zoom = map.getZoom();
      const lat = map.getCenter().lat;
      const radius = Math.max(metresToPixels(HEAT_RADIUS_M, zoom, lat), HEAT_MIN_RADIUS_PX);
      const blur = Math.max(metresToPixels(HEAT_BLUR_M, zoom, lat), HEAT_MIN_BLUR_PX);
      heatLayer.setOptions({ radius: radius, blur: blur });
      histHeatLayer.setOptions({ radius: radius, blur: blur });
    }
    map.on('zoomend', updateHeatRadii);
    updateHeatRadii();

    const layersControl = L.control.layers(null, {
      'Bus markers': markersLayer,
      'Vehicle density heatmap (live)': heatLayer,
      'Vehicle density heatmap (historical)': histHeatLayer
    }, { collapsed: false }).addTo(map);

    const pickerDiv = document.createElement('div');
    pickerDiv.id = 'histWindowPicker';
    pickerDiv.innerHTML = `
      Historical window:
      <select id="histWindowSelect">
        <option value="1">Last hour</option>
        <option value="24" selected>Last 24 hours</option>
        <option value="168">Last 7 days</option>
      </select>
    `;
    layersControl.getContainer().appendChild(pickerDiv);
    const statusDiv = document.createElement('div');
    statusDiv.id = 'histWindowStatus';
    layersControl.getContainer().appendChild(statusDiv);
    L.DomEvent.disableClickPropagation(pickerDiv);

    async function loadHistoricalHeatmap() {
      const windowHours = document.getElementById('histWindowSelect').value;
      statusDiv.textContent = 'Loading…';
      statusDiv.style.color = '#666';
      try {
        const res = await fetch('/api/heatmap?window=' + windowHours);
        const text = await res.text();
        let data;
        try {
          data = JSON.parse(text);
        } catch (parseErr) {
          console.warn('Non-JSON response from /api/heatmap:', text.slice(0, 300));
          statusDiv.textContent = `Bad response (HTTP ${res.status}, ${text.length} bytes): ${text.slice(0, 80)}…`;
          statusDiv.style.color = '#b3261e';
          return;
        }
        const points = data.points || [];
        histHeatLayer.setLatLngs(points);
        if (data.error) {
          statusDiv.textContent = data.error;
          statusDiv.style.color = '#b3261e';
        } else if (points.length === 0) {
          statusDiv.textContent = 'No historical points in this window yet';
          statusDiv.style.color = '#b3261e';
        } else {
          statusDiv.textContent = points.length + ' historical cells loaded';
          statusDiv.style.color = '#666';
        }
      } catch (e) {
        statusDiv.textContent = 'Fetch failed: ' + e;
        statusDiv.style.color = '#b3261e';
        console.warn('Historical heatmap fetch failed', e);
      }
    }
    document.getElementById('histWindowSelect').addEventListener('change', loadHistoricalHeatmap);
    loadHistoricalHeatmap();

    function makeIcon(routeLabel, bearing, outlineColor) {
      const rot = (bearing != null ? bearing : 0) - 90;
      return L.divIcon({
        className: '',
        iconSize: [56, 24],
        iconAnchor: [28, 12],
        popupAnchor: [0, -12],
        html: `
          <div class="bus-marker">
            <div class="bus-pill" style="border-color:${outlineColor};">
              <div class="bus-arrow" style="transform: rotate(${rot}deg);">&#10148;</div>
              <span>${routeLabel}</span>
            </div>
          </div>
        `
      });
    }

    function tooltipContent(v) {
      const routeLabel = v.route_num || v.route_id || '?';
      return v.headsign ? `${routeLabel} to ${v.headsign}` : `Route ${routeLabel}`;
    }

    function popupContent(v) {
      const delayText = (v.delay_min != null)
        ? (v.anomaly ? `${v.delay_min > 0 ? '+' : ''}${v.delay_min} min (flagged as anomalous)` : `${v.delay_min > 0 ? '+' : ''}${v.delay_min} min`)
        : 'No current delay data';
      const speedKmh = (v.speed != null) ? Math.round(v.speed * 3.6) + ' km/h' : 'Speed unavailable';
      const routeLine = v.headsign
        ? `Route ${v.route_num || v.route_id || '?'} to ${v.headsign}`
        : `Route ${v.route_num || v.route_id || '?'}`;
      return `
        <strong>${routeLine}</strong><br>
        ${v.route_operator || 'Unknown operator'}<br>
        Trip ${v.trip_id ?? '?'}<br>
        ${delayText}<br>
        ${speedKmh}
      `;
    }

    function renderVehicles(vehicles) {
      const seen = new Set();
      const heatPoints = [];

      vehicles.forEach(v => {
        if (v.lat == null || v.lon == null) return;
        const key = v.vehicle_id || v.trip_id;
        seen.add(key);

        heatPoints.push([v.lat, v.lon]);

        const routeLabel = v.route_num || v.route_id || '?';
        const icon = makeIcon(routeLabel, v.bearing, v.outline_color || '#888');
        const popup = popupContent(v);
        const tooltip = tooltipContent(v);

        if (markers.has(key)) {
          const m = markers.get(key);
          m.setLatLng([v.lat, v.lon]);
          m.setIcon(icon);
          m.getPopup().setContent(popup);
          m.getTooltip().setContent(tooltip);
        } else {
          const m = L.marker([v.lat, v.lon], { icon })
            .addTo(markersLayer)
            .bindPopup(popup, { className: 'glass-popup' })
            .bindTooltip(tooltip, { direction: 'top', offset: [0, -20], className: 'glass-tooltip' });
          markers.set(key, m);
        }
      });

      for (const [key, m] of markers) {
        if (!seen.has(key)) {
          markersLayer.removeLayer(m);
          markers.delete(key);
        }
      }

      heatLayer.setLatLngs(heatPoints);
    }

    async function pollVehicles() {
      try {
        const res = await fetch('/api/vehicles' + window.location.search);
        const data = await res.json();
        renderVehicles(data.vehicles || []);
      } catch (e) {
        console.warn('Vehicle poll failed', e);
      }
    }

    renderVehicles({{ vehicles_json|safe }});
    setInterval(pollVehicles, 15000);
    setInterval(loadHistoricalHeatmap, 300000);
  </script>
</body>
</html>
"""


@app.route("/ping")
def ping():
    """Render health-check target. Returns instantly, touches nothing."""
    return "pong", 200


@app.route("/health")
def health():
    """Minimal liveness endpoint."""
    return "ok", 200


@app.route("/")
def dashboard():
    if not API_KEY:
        return "TFNSW_API_KEY not set in .env", 500

    data = compute_delay_data(request.args)
    vehicles, map_error = compute_vehicles(data)

    all_rows = data["all_rows"]
    latest_rows = data["latest_rows"]
    agency_names = data["agency_names"]

    observed_prefixes = sorted({r["route_id"].split("_")[0] for r in all_rows if r["route_id"]})[:10]
    agency_debug = (
        f"Loaded {len(agency_names)} operator names. "
        f"Sample loaded IDs: {list(agency_names.keys())[:10]}. "
        f"Sample route-prefix IDs seen in feed: {observed_prefixes}."
    )

    operators = sorted(summarise(latest_rows, "operator"), key=lambda r: -abs(r["mean_min"]))
    routes = sorted(
        summarise(latest_rows, "route_id"),
        key=lambda r: r[data["sort_key"]] if data["ascending"] else -abs(r[data["sort_key"]]),
    )
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
        q_route=data["q_route"],
        q_stop=data["q_stop"],
        q_operator=data["q_operator"],
        agency_error=data["agency_error"],
        agency_debug=agency_debug,
        operators=operators,
        routes=routes,
        worst_trips=worst_trips,
        vehicles=vehicles,
        vehicles_json=json.dumps(vehicles),
        filters_active=data["filters_active"],
        apply_bounds=data["apply_bounds"],
        map_error=map_error,
        color_fill=COLOR_FILL,
        outline_on_time=OUTLINE_ON_TIME,
        outline_late=OUTLINE_LATE,
        outline_early=OUTLINE_EARLY,
        outline_no_data=OUTLINE_NO_DATA,
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
    """Historical density heatmap points. ?window=1|24|168 (hours)."""
    try:
        try:
            window_hours = int(request.args.get("window", 24))
        except ValueError:
            window_hours = 24
        if window_hours not in (1, 24, 168):
            window_hours = 24

        points, error = get_heatmap_points_cached(window_hours)
        return jsonify({"points": points, "window_hours": window_hours, "error": error})
    except Exception as e:
        return jsonify({"points": [], "window_hours": None, "error": f"Server error: {e}"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)