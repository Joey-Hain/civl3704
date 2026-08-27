"""
Web dashboard for TfNSW GTFS-realtime delay/variance data.

Fetches the live trip-update feed on each page load, computes delay stats
per operator, route and trip, and renders a sortable HTML table. Also
appends every pull to delay_log.csv (same as timetable_variance.py) so you
build up history.

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

ROUTE SHAPES (new): drawn as thin blue polylines under the bus markers.
The previous attempt at this loaded shapes.txt for the *whole* static GTFS
bundle into memory (via csv.reader(list(...)) over a fully-decoded string)
and blew through Render.com's 512MB free-tier cap - shapes.txt for the
whole NSW bus network is tens of MB of rows, almost all of which are never
displayed. This version instead:
  1. only ever fetches shapes for the *specific* shape_ids belonging to
     trip_ids currently visible on the map (typically well under a
     hundred), never the whole file's contents;
  2. streams shapes.txt row-by-row via io.TextIOWrapper directly over the
     open zip member, so the full decoded CSV text is never held in memory
     at once - only the (small) set of matched points survives each pass;
  3. simplifies each shape with a lightweight Douglas-Peucker pass before
     caching it, since raw GTFS shapes are far denser than a ~500px-wide
     Leaflet pane needs;
  4. caches results in a hard-capped, LRU-evicted in-memory dict
     (MAX_CACHED_SHAPES) so the cache itself can't grow unbounded across a
     long-running process even if lots of different routes get viewed.

LIQUID-GLASS POPUPS: purely a frontend CSS concern (backdrop-filter blur on
the Leaflet popup/tooltip chrome) - this costs nothing server-side. It was
rolled back together with the shapes work last time, so it's reinstated
here for both the hover tooltip and the click popup.

COLOUR SCHEME (changed): every bus marker now has the same blue fill
(brand colour), with delay status shown via the marker's OUTLINE colour
instead of swapping the fill. This keeps the map visually calm (one colour
family) while still making outliers scannable by ring colour.

Setup:
    pip install flask requests python-dotenv gtfs-realtime-bindings tzdata

.env file:
    TFNSW_API_KEY=your_api_key_here
    TFNSW_GTFS_RT_URL=https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses

Usage:
    python app.py
    then open http://localhost:5000
"""

import csv
import io
import json
import os
import statistics
import zipfile
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request
from google.transit import gtfs_realtime_pb2

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
DATA_DIR = Path("CIVL3704")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = DATA_DIR / "delay_log.csv"
AGENCY_CACHE_FILE = DATA_DIR / "agency_names.json"
AGENCY_CACHE_MAX_AGE = timedelta(hours=24)
ANOMALY_ABS_SEC = 3600  # readings beyond this magnitude are flagged as likely stale/broken
ON_TIME_EARLY_SEC = -60   # 1 min early
ON_TIME_LATE_SEC = 300    # 5 min late — standard industry on-time window

load_dotenv()
API_KEY = os.getenv("TFNSW_API_KEY")
FEED_URL = os.getenv("TFNSW_GTFS_RT_URL", "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses")
SCHEDULE_URL = os.getenv("TFNSW_GTFS_SCHEDULE_URL", "https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses")

# Deliberately hardcoded — NOT sourced from .env's TFNSW_GTFS_RT_URL, which
# is the trip-update feed above. Vehicle positions are a separate GTFS-RT
# product on the TfNSW developer portal with their own subscription.
VEHICLE_POS_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"

# --- Colour scheme: single blue fill, delay status carried by outline ---
COLOR_FILL = "#00B3F0"          # every marker's pill background, regardless of status
OUTLINE_ON_TIME = "#ffffff"     # pale blue ring — reads as "normal" against the blue fill
OUTLINE_LATE = "#B3261E"
OUTLINE_EARLY = "#1E6B3C"
OUTLINE_NO_DATA = "#888888"
SHAPE_LINE_COLOR = "#00B3F0"    # route polylines reuse the same blue, at low opacity

# Default radius filter for the map/API — the vehiclepos/buses feed is
# STATEWIDE (thousands of vehicles across all NSW bus contract regions),
# and shipping all of them as JSON on every request/poll produces
# multi-hundred-KB payloads that are slow on weak connections and heavy
# to render as markers. Restrict to within SYDNEY_RADIUS_KM of the CBD by
# default; pass ?bounds=0 to disable and see the full unfiltered feed.
SYDNEY_CBD = (-33.8688, 151.2093)  # lat, lon
SYDNEY_RADIUS_KM = 10
 
# --- Shape cache bounds — see module docstring ---
MAX_CACHED_SHAPES = 300          # hard cap on distinct shape_ids held in memory at once
SHAPE_SIMPLIFY_TOLERANCE_DEG = 0.00006

SCHEDULE_ZIP_CACHE_TTL = timedelta(minutes=15)
 
def haversine_km(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, asin, sqrt
    r = 6371.0  # Earth radius, km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))
 
 
def simplify_polyline(points: list[tuple[float, float]], tolerance: float) -> list[tuple[float, float]]:
    """Ramer-Douglas-Peucker simplification, iterative (no recursion limit
    risk on long shapes). points/return are [(lat, lon), ...]. Cuts point
    count substantially for typical GTFS shapes without visibly changing
    the drawn route, which is most of what keeps the shape cache small."""
    if len(points) < 3:
        return points
 
    def perp_dist(pt, a, b):
        (x, y), (x1, y1), (x2, y2) = pt, a, b
        dx, dy = x2 - x1, y2 - y1
        if dx == dy == 0:
            return ((x - x1) ** 2 + (y - y1) ** 2) ** 0.5
        t = ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy)
        t = max(0.0, min(1.0, t))
        proj_x, proj_y = x1 + t * dx, y1 + t * dy
        return ((x - proj_x) ** 2 + (y - proj_y) ** 2) ** 0.5
 
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        if end - start < 2:
            continue
        a, b = points[start], points[end]
        max_dist, max_idx = -1.0, -1
        for i in range(start + 1, end):
            d = perp_dist(points[i], a, b)
            if d > max_dist:
                max_dist, max_idx = d, i
        if max_dist > tolerance:
            keep[max_idx] = True
            stack.append((start, max_idx))
            stack.append((max_idx, end))
    return [p for p, k in zip(points, keep) if k]
 
 
app = Flask(__name__)
 
 
def load_schedule_lookups() -> tuple[dict[str, str], dict[str, str], dict[str, str], str | None]:
    """Return ({agency_id: agency_name}, {trip_id: trip_headsign},
    {trip_id: shape_id}, error_message_or_None).
 
    Tries the static GTFS schedule bundle, which may come back either as a
    flat GTFS zip (agency.txt/trips.txt at top level) or a zip-of-zips (one
    nested zip per contract region, each with its own agency.txt/trips.txt)
    — TfNSW has changed this shape before, so we handle both. Caches
    successful results for 24h. If the fetch fails, we surface why (e.g.
    401/403 usually means the API key isn't subscribed to the "Timetables
    Complete GTFS" / bus schedule product separately from the realtime
    product) instead of silently falling back to numeric IDs.
 
    trip_headsign (from trips.txt) is what's physically shown on the bus's
    destination sign — e.g. "Bondi Beach" — and is direction-specific,
    unlike routes.txt's route_long_name which is usually a fixed "A to B"
    description that doesn't tell you which way a given trip is currently
    headed. shape_id (also from trips.txt) is what lets /api/shapes later
    find the right polyline for a trip without re-parsing trips.txt.
 
    NOTE: unlike shapes.txt (see get_shapes_for_trip_ids below), agency.txt
    and trips.txt are both small enough system-wide to read in full and
    cache — it's specifically shapes.txt that's too big to ever load whole.
    """
    if AGENCY_CACHE_FILE.exists():
        try:
            cached = json.loads(AGENCY_CACHE_FILE.read_text())
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            if datetime.now(tz=SYDNEY_TZ) - fetched_at < AGENCY_CACHE_MAX_AGE:
                return (cached["agencies"], cached.get("trip_headsigns", {}),
                        cached.get("trip_shapes", {}), None)
        except Exception:
            pass  # corrupt cache, refetch below
 
    def parse_csv_txt(text: str, key_col: str, value_col: str) -> dict[str, str]:
        result = {}
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return result
        header = [h.strip() for h in rows[0]]
        if key_col not in header or value_col not in header:
            return result
        key_idx = header.index(key_col)
        val_idx = header.index(value_col)
        for row in rows[1:]:
            if len(row) > max(key_idx, val_idx) and row[val_idx].strip():
                result[row[key_idx].strip()] = row[val_idx].strip()
        return result
 
    try:
        resp = requests.get(SCHEDULE_URL, headers={"Authorization": f"apikey {API_KEY}"}, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Schedule endpoint returned HTTP {resp.status_code}. "
                f"This usually means the API key isn't subscribed to the bus schedule/timetable "
                f"product (separate from GTFS Realtime) on the TfNSW developer portal."
            )
        outer = zipfile.ZipFile(io.BytesIO(resp.content))
        names = outer.namelist()
 
        agencies: dict[str, str] = {}
        trip_headsigns: dict[str, str] = {}
        trip_shapes: dict[str, str] = {}
 
        def parse_bundle(zf: zipfile.ZipFile) -> None:
            if "agency.txt" in zf.namelist():
                agencies.update(parse_csv_txt(zf.read("agency.txt").decode("utf-8-sig"), "agency_id", "agency_name"))
            if "trips.txt" in zf.namelist():
                trips_text = zf.read("trips.txt").decode("utf-8-sig")
                trip_headsigns.update(parse_csv_txt(trips_text, "trip_id", "trip_headsign"))
                trip_shapes.update(parse_csv_txt(trips_text, "trip_id", "shape_id"))
 
        if "agency.txt" in names or "trips.txt" in names:
            # flat bundle
            parse_bundle(outer)
        else:
            # zip-of-zips, one per contract region
            for name in names:
                if name.endswith(".zip"):
                    inner = zipfile.ZipFile(io.BytesIO(outer.read(name)))
                    parse_bundle(inner)
 
        if not agencies:
            raise RuntimeError("Downloaded schedule bundle but found no agency.txt / no agency rows in it.")
 
        AGENCY_CACHE_FILE.write_text(json.dumps({
            "fetched_at": datetime.now(tz=SYDNEY_TZ).isoformat(),
            "agencies": agencies,
            "trip_headsigns": trip_headsigns,
            "trip_shapes": trip_shapes,
        }))
        return agencies, trip_headsigns, trip_shapes, None
    except Exception as e:
        if AGENCY_CACHE_FILE.exists():
            try:
                cached = json.loads(AGENCY_CACHE_FILE.read_text())
                return (cached["agencies"], cached.get("trip_headsigns", {}),
                        cached.get("trip_shapes", {}), f"Using stale cached names ({e})")
            except Exception:
                pass
        return {}, {}, {}, str(e)
 
 
# --- Shape cache: LRU dict capped at MAX_CACHED_SHAPES, see module docstring ---
_shape_cache: "OrderedDict[str, list[list[float]]]" = OrderedDict()
 
# --- Short-TTL cache of the raw schedule zip bytes, used only by the shape
# lookup path (see SCHEDULE_ZIP_CACHE_TTL above for why). Distinct from the
# agency/trip-headsign disk cache in load_schedule_lookups, which parses
# and discards the zip immediately rather than keeping bytes around. ---
_schedule_zip_cache = {"content": None, "fetched_at": None}
 
 
def _get_schedule_zip_bytes() -> bytes:
    """Return the schedule zip's raw bytes, reusing a recent download if
    within SCHEDULE_ZIP_CACHE_TTL. Raises requests.RequestException or
    RuntimeError (non-200) on failure, same as a direct requests.get would."""
    now = datetime.now(tz=SYDNEY_TZ)
    fetched_at = _schedule_zip_cache["fetched_at"]
    if fetched_at is not None and (now - fetched_at) < SCHEDULE_ZIP_CACHE_TTL:
        return _schedule_zip_cache["content"]
 
    resp = requests.get(SCHEDULE_URL, headers={"Authorization": f"apikey {API_KEY}"}, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Schedule endpoint returned HTTP {resp.status_code}")
    _schedule_zip_cache.update(content=resp.content, fetched_at=now)
    return resp.content
 
 
def _shape_cache_get(shape_id: str):
    if shape_id in _shape_cache:
        _shape_cache.move_to_end(shape_id)  # mark as recently used
        return _shape_cache[shape_id]
    return None
 
 
def _shape_cache_put(shape_id: str, points: list[list[float]]) -> None:
    _shape_cache[shape_id] = points
    _shape_cache.move_to_end(shape_id)
    while len(_shape_cache) > MAX_CACHED_SHAPES:
        _shape_cache.popitem(last=False)  # evict least-recently-used
 
 
def stream_shapes_for_ids(zf: zipfile.ZipFile, shape_ids_needed: set[str]) -> dict[str, list[list[float]]]:
    """Stream shapes.txt from an already-open zip member, keeping only rows
    whose shape_id is in shape_ids_needed. Never materialises the full file
    as a string or a list of rows — io.TextIOWrapper + csv.reader iterate
    the member's bytes on demand, so peak memory is roughly "one row" plus
    the (small) accumulated points for the requested shapes only.
    Returns {shape_id: [[lat, lon], ...]} in shape_pt_sequence order,
    already simplified."""
    raw_points: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    with zf.open("shapes.txt") as raw:
        text_stream = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.reader(text_stream)
        header = next(reader, None)
        if header is None:
            return {}
        idx = {h.strip(): i for i, h in enumerate(header)}
        required = ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence")
        if not all(c in idx for c in required):
            return {}
        sid_i, lat_i, lon_i, seq_i = (idx[c] for c in required)
        max_col = max(sid_i, lat_i, lon_i, seq_i)
        for row in reader:
            if len(row) <= max_col:
                continue
            sid = row[sid_i]
            if sid not in shape_ids_needed:
                continue
            try:
                raw_points[sid].append((int(row[seq_i]), float(row[lat_i]), float(row[lon_i])))
            except ValueError:
                continue  # malformed row — skip rather than fail the whole pass
 
    out: dict[str, list[list[float]]] = {}
    for sid, pts in raw_points.items():
        pts.sort(key=lambda p: p[0])
        latlon = [(lat, lon) for _, lat, lon in pts]
        simplified = simplify_polyline(latlon, SHAPE_SIMPLIFY_TOLERANCE_DEG)
        out[sid] = [[lat, lon] for lat, lon in simplified]
    return out
 
 
def get_shapes_for_trip_ids(trip_ids: set[str], trip_shapes: dict[str, str]) -> tuple[dict[str, list[list[float]]], str | None]:
    """Resolve trip_ids -> shape geometry, serving from the capped cache
    where possible and reusing the short-TTL cached schedule zip (see
    _get_schedule_zip_bytes) for whatever's missing, instead of hitting
    TfNSW fresh on every single miss — that repeated-download pattern is
    what caused page-load timeouts under continuous polling (see
    SCHEDULE_ZIP_CACHE_TTL above). This is the only place shapes.txt gets
    touched, and only for the shape_ids actually needed right now."""
    wanted_shape_ids = {trip_shapes[t] for t in trip_ids if t in trip_shapes and trip_shapes[t]}
    if not wanted_shape_ids:
        # Common cause: trips.txt has no shape_id column at all, or it's
        # blank for these trips. Surfaced to the caller instead of just
        # returning {} silently, since this is the #1 reason nothing draws.
        return {}, (
            f"No shape_id found for any of {len(trip_ids)} requested trip_ids "
            f"(schedule has shape_id mapped for {len(trip_shapes)} trips total)."
        )
 
    result: dict[str, list[list[float]]] = {}
    missing: set[str] = set()
    for sid in wanted_shape_ids:
        cached = _shape_cache_get(sid)
        if cached is not None:
            result[sid] = cached
        else:
            missing.add(sid)
 
    if not missing:
        return result, None
 
    try:
        zip_bytes = _get_schedule_zip_bytes()
        outer = zipfile.ZipFile(io.BytesIO(zip_bytes))
        names = outer.namelist()
 
        found: dict[str, list[list[float]]] = {}
        if "shapes.txt" in names:
            found.update(stream_shapes_for_ids(outer, missing))
        else:
            # zip-of-zips: check each region, stop early once everything's found
            for name in names:
                if not missing - found.keys():
                    break
                if name.endswith(".zip"):
                    inner = zipfile.ZipFile(io.BytesIO(outer.read(name)))
                    if "shapes.txt" in inner.namelist():
                        found.update(stream_shapes_for_ids(inner, missing - found.keys()))
 
        for sid, pts in found.items():
            _shape_cache_put(sid, pts)
            result[sid] = pts
 
        still_missing = missing - found.keys()
        error = None
        if still_missing:
            error = (
                f"{len(still_missing)} shape_id(s) requested but not found in shapes.txt "
                f"(e.g. {sorted(still_missing)[:3]}) — either not present in the bundle "
                f"or the zip-of-zips region containing them wasn't matched."
            )
        return result, error
    except Exception as e:
        return result, f"Shape fetch failed: {type(e).__name__}: {e}"
 
 
def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    headers = {"Authorization": f"apikey {API_KEY}"}
    response = requests.get(FEED_URL, headers=headers, timeout=15)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed
 
 
def fetch_vehicle_feed() -> gtfs_realtime_pb2.FeedMessage:
    headers = {"Authorization": f"apikey {API_KEY}"}
    response = requests.get(VEHICLE_POS_URL, headers=headers, timeout=15)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed
 
 
def extract_rows(feed: gtfs_realtime_pb2.FeedMessage, agency_names: dict[str, str]) -> list[dict]:
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
 
 
def latest_reading_per_trip(rows: list[dict]) -> dict[str, dict]:
    """Collapse to one reading per trip — the freshest, i.e. the stop with the
    lowest stop_sequence still in the feed (TfNSW drops stops once a bus
    passes them, so the lowest remaining sequence is the newest data point)."""
    latest: dict[str, dict] = {}
    for r in rows:
        existing = latest.get(r["trip_id"])
        if existing is None or r["stop_sequence"] < existing["stop_sequence"]:
            latest[r["trip_id"]] = r
    return latest
 
 
def extract_vehicles(feed: gtfs_realtime_pb2.FeedMessage, agency_names: dict[str, str],
                      trip_headsigns: dict[str, str] | None = None) -> list[dict]:
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
            "headsign": trip_headsigns.get(trip_id),  # e.g. "Bondi Beach" — None if not in the schedule bundle
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "bearing": v.position.bearing if v.HasField("position") else None,
            "speed": v.position.speed if v.HasField("position") else None,
        })
    return vehicles
 
 
def merge_vehicle_delays(vehicles: list[dict], delay_by_trip: dict[str, dict]) -> None:
    """Attach delay info to each vehicle dict in place, joined by trip_id.
    Fill colour is always COLOR_FILL now — only outline_color varies with
    status — see module docstring on the colour scheme change."""
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
 
 
def append_to_log(rows: list[dict]) -> None:
    file_exists = LOG_FILE.exists()
    with LOG_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["pulled_at", "operator", "route_id", "trip_id", "stop_id", "stop_sequence", "delay", "anomaly"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
 
 
def summarise(rows: list[dict], key: str) -> list[dict]:
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
 
 
def split_route(route_id: str, agency_names: dict[str, str]) -> tuple[str, str]:
    """Split '2504_601' into ('601', 'Transdev NSW') — route number and operator name."""
    if "_" not in route_id:
        return route_id, ""
    agency_id, route_num = route_id.split("_", 1)
    operator = agency_names.get(agency_id, agency_id)
    return route_num, operator
 
 
# Short-TTL in-memory caches, shared between the dashboard page load and the
# /api/vehicles poll. Without this, every 15s poll (from every open tab) was
# independently re-fetching BOTH TfNSW feeds and re-appending the full
# trip-update pull to delay_log.csv — the log growing unbounded on every
# poll (not just page loads) was the main driver of the slowdown/timeouts.
# TTL is kept just under the client's 15s poll interval so data still feels
# live while bursts of concurrent requests (multiple tabs, page load +
# poll landing close together) reuse one fetch instead of triggering their own.
CACHE_TTL_SECONDS = 12
_rows_cache = {"all_rows": None, "agency_names": None, "trip_headsigns": None, "trip_shapes": None,
               "agency_error": None, "fetched_at": None}
_vehicles_cache = {"vehicles": None, "fetched_at": None}
 
 
def get_all_rows_cached():
    """Fetch+parse the trip-update feed, reusing a recent result if within
    CACHE_TTL_SECONDS. Only logs to delay_log.csv when an actual fetch
    happens, not on cache hits."""
    now = datetime.now(tz=SYDNEY_TZ)
    cached_at = _rows_cache["fetched_at"]
    if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
        return (_rows_cache["all_rows"], _rows_cache["agency_names"], _rows_cache["trip_headsigns"],
                _rows_cache["trip_shapes"], _rows_cache["agency_error"])
 
    agency_names, trip_headsigns, trip_shapes, agency_error = load_schedule_lookups()
    feed = fetch_feed()
    all_rows = extract_rows(feed, agency_names)
    append_to_log(all_rows)  # only on an actual fetch, not every poll
 
    _rows_cache.update(all_rows=all_rows, agency_names=agency_names, trip_headsigns=trip_headsigns,
                        trip_shapes=trip_shapes, agency_error=agency_error, fetched_at=now)
    return all_rows, agency_names, trip_headsigns, trip_shapes, agency_error
 
 
def get_vehicles_cached(agency_names, trip_headsigns):
    """Fetch+parse the vehicle-position feed, reusing a recent result if
    within CACHE_TTL_SECONDS. Returns a fresh copy of dicts each call so
    per-request delay-merging/filtering never mutates the cached objects."""
    now = datetime.now(tz=SYDNEY_TZ)
    cached_at = _vehicles_cache["fetched_at"]
    if cached_at is not None and (now - cached_at).total_seconds() < CACHE_TTL_SECONDS:
        return [dict(v) for v in _vehicles_cache["vehicles"]]
 
    vfeed = fetch_vehicle_feed()
    vehicles = extract_vehicles(vfeed, agency_names, trip_headsigns)
    _vehicles_cache.update(vehicles=vehicles, fetched_at=now)
    return [dict(v) for v in vehicles]
 
 
def compute_delay_data(args):
    """Shared by the dashboard page and /api/vehicles: fetches the trip-update
    feed (via the short-TTL cache above), applies the route/stop/operator/
    hide_anomalies filters from the request, and returns everything both need."""
    hide_anomalies = args.get("hide_anomalies", "1") == "1"
    sort_key = args.get("sort", "stdev_min")
    ascending = args.get("asc") == "1"
    q_route = args.get("route", "").strip().lower()
    q_stop = args.get("stop", "").strip().lower()
    q_operator = args.get("operator", "").strip().lower()
    apply_bounds = args.get("bounds", "1") == "1"
 
    all_rows, agency_names, trip_headsigns, trip_shapes, agency_error = get_all_rows_cached()
 
    rows = [r for r in all_rows if not (hide_anomalies and r["anomaly"])]
 
    if q_route:
        rows = [r for r in rows if q_route in r["route_id"].lower()]
    if q_operator:
        rows = [r for r in rows if q_operator in r["operator"].lower()]
    if q_stop:
        # match against ALL stops each trip touches (past+upcoming in the feed),
        # not just its most recent reading, then keep every row for those trips
        # so the latest-per-trip dedupe below still finds their current status
        matching_trip_ids = {r["trip_id"] for r in rows if q_stop in r["stop_id"].lower()}
        rows = [r for r in rows if r["trip_id"] in matching_trip_ids]
 
    latest_by_trip = latest_reading_per_trip(rows)
    latest_rows = list(latest_by_trip.values())
 
    # Unfiltered, freshest-per-trip delay lookup — used to colour/annotate
    # every vehicle on the map, independent of which trips currently pass
    # the route/stop/operator/hide_anomalies filters above.
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
        "trip_shapes": trip_shapes,
        "agency_error": agency_error,
        "all_rows": all_rows,
        "latest_by_trip": latest_by_trip,
        "latest_rows": latest_rows,
        "delay_by_trip_all": delay_by_trip_all,
        "filters_active": bool(q_route or q_stop or q_operator),
        "apply_bounds": apply_bounds,
    }
 
 
def compute_vehicles(data):
    """Fetch vehicle positions (via the short-TTL cache above), join delays
    by trip_id, apply the same route/stop/operator filters already resolved
    in `data` (via latest_by_trip's trip_id set) so the map matches the
    tables below it."""
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
    border: 2px solid #999; /* overridden inline per-swatch to show its ring colour */
  }
  .map-error { color: var(--late); font-size: 0.85rem; margin-top: 8px; }
 
  /* Bus marker: flat pill, arrow sits inline next to the route number.
     Fill is always the brand blue; the ring (border-color) carries delay
     status — see COLOUR SCHEME note in app.py. */
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
    border: 2.5px solid #888; /* replaced inline with the status outline colour */
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
 
  /* Frosted-glass chrome, shared by the hover tooltip and the click popup.
     Leaflet's `className` option puts this class on .leaflet-tooltip for
     tooltips but on the outer .leaflet-popup for popups, so both selector
     forms below are needed. */
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
  .glass-tooltip::before { display: none; } /* hide Leaflet's default solid-colour pointer arrow */
 
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
    const map = L.map('dashmap').setView([-33.8688, 151.2093], 11); // Sydney
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);
 
    const markers = new Map();       // trip_id/vehicle_id -> Leaflet marker
    const shapePolylines = new Map(); // shape_id -> Leaflet polyline
    const SHAPE_LINE_COLOR = "{{ shape_line_color }}";
 
    function makeIcon(routeLabel, bearing, outlineColor) {
      const rot = (bearing != null ? bearing : 0) - 90; // glyph points right by default; GTFS bearing is clockwise from north
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
      vehicles.forEach(v => {
        if (v.lat == null || v.lon == null) return;
        const key = v.vehicle_id || v.trip_id;
        seen.add(key);
 
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
            .addTo(map)
            .bindPopup(popup, { className: 'glass-popup' })
            .bindTooltip(tooltip, { direction: 'top', offset: [0, -20], className: 'glass-tooltip' });
          markers.set(key, m);
        }
      });
 
      for (const [key, m] of markers) {
        if (!seen.has(key)) {
          map.removeLayer(m);
          markers.delete(key);
        }
      }
 
      // Route shapes: fetch only for trip_ids currently on screen, skip any
      // shape_id we've already drawn this session (geometry doesn't change).
      // POST with a JSON body (not GET+querystring) — see api_shapes() for
      // why: a few hundred trip_ids blows past the ~4KB GET request-line
      // limit most WSGI servers enforce, which was the original bug here.
      const tripIds = [...new Set(vehicles.map(v => v.trip_id).filter(Boolean))];
      if (tripIds.length) {
        fetch('/api/shapes', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ trip_ids: tripIds }),
        })
          .then(res => res.json().then(data => ({ ok: res.ok, status: res.status, data })))
          .then(({ ok, status, data }) => {
            if (!ok) console.warn(`[shapes] HTTP ${status}:`, data.error);
            if (data.error) console.warn('[shapes] server reported:', data.error);
            const entries = Object.entries(data.shapes || {});
            console.log(`[shapes] got ${entries.length} shape(s) for ${tripIds.length} trip_id(s)`);
            for (const [shapeId, points] of entries) {
              if (shapePolylines.has(shapeId) || !points.length) continue;
              const pl = L.polyline(points, {
                color: SHAPE_LINE_COLOR,
                weight: 3,
                opacity: 0.35,
                interactive: false,
              }).addTo(map);
              pl.bringToBack();
              shapePolylines.set(shapeId, pl);
            }
          })
          .catch(e => console.warn('Shape fetch request failed', e));
      }
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
 
    // Initial paint uses the vehicles fetched at page-load (server-rendered),
    // then polls independently every 15s so the map stays live without
    // reloading the tables below.
    renderVehicles({{ vehicles_json|safe }});
    setInterval(pollVehicles, 15000);
  </script>
</body>
</html>
"""
 
 
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
        shape_line_color=SHAPE_LINE_COLOR,
    )
 
 
@app.route("/api/vehicles")
def api_vehicles():
    if not API_KEY:
        return jsonify({"error": "TFNSW_API_KEY not set in .env"}), 500
    data = compute_delay_data(request.args)
    vehicles, map_error = compute_vehicles(data)
    return jsonify({"vehicles": vehicles, "error": map_error})
 
 
@app.route("/api/shapes", methods=["POST"])
def api_shapes():
    """Return simplified route-shape polylines for the given trip_ids only —
    see get_shapes_for_trip_ids and the module docstring for why this never
    loads shapes.txt in full.
 
    POST (not GET) deliberately: with a few hundred vehicles on screen, the
    trip_id list is easily 4-8KB — comfortably over the ~4KB request-line
    limit most WSGI servers (Gunicorn included) enforce on GET query
    strings, which is exactly what was happening here (the 400s were
    Gunicorn rejecting the request before Flask ever saw it, hence the HTML
    error page instead of JSON). A POST body has no such limit.
    Usage: POST /api/shapes  body: {"trip_ids": ["id1", "id2", ...]}
 
    The whole body is wrapped in try/except: get_shapes_for_trip_ids already
    catches its own failures, but get_all_rows_cached() below does a live
    fetch to the trip-update feed and can genuinely raise (network hiccup,
    TfNSW rate limit, etc). Without this wrapper that turns into Flask's
    generic HTML 500 page, which is why the client saw "Unexpected token
    '<'" instead of a JSON error — this makes any future failure visible
    as JSON with a real message and a traceback in the server logs,
    instead of a silent opaque 500."""
    if not API_KEY:
        return jsonify({"error": "TFNSW_API_KEY not set in .env"}), 500
    try:
        body = request.get_json(silent=True) or {}
        trip_ids = {t for t in body.get("trip_ids", []) if t}
        if not trip_ids:
            return jsonify({"shapes": {}, "error": None})
        _, _, _, trip_shapes, _ = get_all_rows_cached()
        shapes, error = get_shapes_for_trip_ids(trip_ids, trip_shapes)
        if error:
            print(f"[/api/shapes] {error}")  # visible in Render logs
        return jsonify({"shapes": shapes, "error": error})
    except Exception as e:
        import traceback
        traceback.print_exc()  # full traceback in Render logs
        return jsonify({"shapes": {}, "error": f"{type(e).__name__}: {e}"}), 500
 
 
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)