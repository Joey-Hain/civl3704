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
from collections import defaultdict
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

# Marker colours for the map, reusing the same on-time thresholds as the
# rest of the dashboard.
COLOR_ON_TIME = "#00B5EF"
COLOR_LATE = "#b3261e"
COLOR_EARLY = "#1e6b3c"
COLOR_NO_DATA = "#888888"

app = Flask(__name__)


def load_agency_names() -> tuple[dict[str, str], str | None]:
    """Return ({agency_id: agency_name}, error_message_or_None).

    Tries the static GTFS schedule bundle, which may come back either as a
    flat GTFS zip (agency.txt at top level) or a zip-of-zips (one nested
    zip per contract region, each with its own agency.txt) — TfNSW has
    changed this shape before, so we handle both. Caches successful results
    for 24h. If the fetch fails, we surface why (e.g. 401/403 usually means
    the API key isn't subscribed to the "Timetables Complete GTFS" / bus
    schedule product separately from the realtime product) instead of
    silently falling back to numeric IDs.
    """
    if AGENCY_CACHE_FILE.exists():
        try:
            cached = json.loads(AGENCY_CACHE_FILE.read_text())
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            if datetime.now(tz=SYDNEY_TZ) - fetched_at < AGENCY_CACHE_MAX_AGE:
                return cached["agencies"], None
        except Exception:
            pass  # corrupt cache, refetch below

    def parse_agency_txt(text: str) -> dict[str, str]:
        result = {}
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return result
        header = [h.strip() for h in rows[0]]
        if "agency_id" not in header or "agency_name" not in header:
            return result
        id_idx = header.index("agency_id")
        name_idx = header.index("agency_name")
        for row in rows[1:]:
            if len(row) > max(id_idx, name_idx):
                result[row[id_idx].strip()] = row[name_idx].strip()
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

        agencies = {}
        if "agency.txt" in names:
            # flat bundle
            agencies = parse_agency_txt(outer.read("agency.txt").decode("utf-8-sig"))
        else:
            # zip-of-zips, one per contract region
            for name in names:
                if name.endswith(".zip"):
                    inner = zipfile.ZipFile(io.BytesIO(outer.read(name)))
                    if "agency.txt" in inner.namelist():
                        agencies.update(parse_agency_txt(inner.read("agency.txt").decode("utf-8-sig")))

        if not agencies:
            raise RuntimeError("Downloaded schedule bundle but found no agency.txt / no agency rows in it.")

        AGENCY_CACHE_FILE.write_text(json.dumps({
            "fetched_at": datetime.now(tz=SYDNEY_TZ).isoformat(),
            "agencies": agencies,
        }))
        return agencies, None
    except Exception as e:
        if AGENCY_CACHE_FILE.exists():
            try:
                return json.loads(AGENCY_CACHE_FILE.read_text())["agencies"], f"Using stale cached names ({e})"
            except Exception:
                pass
        return {}, str(e)


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


def extract_vehicles(feed: gtfs_realtime_pb2.FeedMessage, agency_names: dict[str, str]) -> list[dict]:
    vehicles = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.HasField("position"):
            continue
        route_id = v.trip.route_id if v.HasField("trip") else ""
        route_num, route_operator = split_route(route_id, agency_names) if route_id else ("", "")
        vehicles.append({
            "trip_id": v.trip.trip_id if v.HasField("trip") else None,
            "vehicle_id": v.vehicle.id if v.HasField("vehicle") else entity.id,
            "route_id": route_id,
            "route_num": route_num,
            "route_operator": route_operator,
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "bearing": v.position.bearing if v.HasField("position") else None,
            "speed": v.position.speed if v.HasField("position") else None,
        })
    return vehicles


def merge_vehicle_delays(vehicles: list[dict], delay_by_trip: dict[str, dict]) -> None:
    """Attach delay info to each vehicle dict in place, joined by trip_id."""
    for veh in vehicles:
        d = delay_by_trip.get(veh["trip_id"])
        if d is None:
            veh["delay_sec"] = None
            veh["delay_min"] = None
            veh["anomaly"] = False
            veh["on_time"] = None
            veh["color"] = COLOR_NO_DATA
            continue
        veh["delay_sec"] = d["delay"]
        veh["delay_min"] = round(d["delay"] / 60, 1)
        veh["anomaly"] = d["anomaly"]
        on_time = ON_TIME_EARLY_SEC <= d["delay"] <= ON_TIME_LATE_SEC
        veh["on_time"] = on_time
        if d["anomaly"]:
            veh["color"] = COLOR_NO_DATA
        elif on_time:
            veh["color"] = COLOR_ON_TIME
        elif d["delay"] > 0:
            veh["color"] = COLOR_LATE
        else:
            veh["color"] = COLOR_EARLY


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


def compute_delay_data(args):
    """Shared by the dashboard page and /api/vehicles: fetches the trip-update
    feed, applies the route/stop/operator/hide_anomalies filters from the
    request, and returns everything both need."""
    hide_anomalies = args.get("hide_anomalies", "1") == "1"
    sort_key = args.get("sort", "stdev_min")
    ascending = args.get("asc") == "1"
    q_route = args.get("route", "").strip().lower()
    q_stop = args.get("stop", "").strip().lower()
    q_operator = args.get("operator", "").strip().lower()

    agency_names, agency_error = load_agency_names()
    feed = fetch_feed()
    all_rows = extract_rows(feed, agency_names)
    append_to_log(all_rows)

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
        "agency_error": agency_error,
        "all_rows": all_rows,
        "latest_by_trip": latest_by_trip,
        "latest_rows": latest_rows,
        "delay_by_trip_all": delay_by_trip_all,
        "filters_active": bool(q_route or q_stop or q_operator),
    }


def compute_vehicles(data):
    """Fetch vehicle positions, join delays by trip_id, apply the same
    route/stop/operator filters already resolved in `data` (via
    latest_by_trip's trip_id set) so the map matches the tables below it."""
    try:
        vfeed = fetch_vehicle_feed()
    except requests.RequestException as e:
        return [], str(e)

    vehicles = extract_vehicles(vfeed, data["agency_names"])
    merge_vehicle_delays(vehicles, data["delay_by_trip_all"])

    if data["filters_active"]:
        allowed_trip_ids = set(data["latest_by_trip"].keys())
        vehicles = [v for v in vehicles if v["trip_id"] in allowed_trip_ids]

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

  #dashmap { height: 440px; border: 1px solid var(--line); margin-top: 10px; background: #e5e3dc; }
  .map-legend {
    display: flex; gap: 16px; align-items: center;
    font-size: 0.8rem; color: var(--muted); margin-top: 8px; flex-wrap: wrap;
  }
  .map-legend .swatch {
    display: inline-block; width: 10px; height: 10px; border-radius: 2px;
    margin-right: 4px; vertical-align: middle;
  }
  .map-error { color: var(--late); font-size: 0.85rem; margin-top: 8px; }

  /* Bus marker: flat pill, arrow sits inline next to the route number */
  .bus-marker { position: relative; width: 56px; height: 24px; }
  .bus-pill {
    position: absolute;
    top: 0; left: 50%;
    transform: translateX(-50%);
    display: flex;
    align-items: center;
    gap: 4px;
    color: #fff;
    font: 600 11px/1 -apple-system, Helvetica, Arial, sans-serif;
    padding: 5px 7px;
    border-radius: 7px;
    border: 2px solid #fff;
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
    <span><span class="swatch" style="background:{{ color_on_time }}"></span>On time</span>
    <span><span class="swatch" style="background:{{ color_late }}"></span>Late</span>
    <span><span class="swatch" style="background:{{ color_early }}"></span>Early</span>
    <span><span class="swatch" style="background:{{ color_no_data }}"></span>No delay data / anomalous</span>
    <span>{{ vehicles|length }} vehicles shown{% if filters_active %} (filtered to match route/stop/operator above){% endif %}</span>
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

    const markers = new Map(); // trip_id/vehicle_id -> Leaflet marker

    function makeIcon(routeLabel, bearing, color) {
      const rot = (bearing != null ? bearing : 0) - 90; // glyph points right by default; GTFS bearing is clockwise from north
      return L.divIcon({
        className: '',
        iconSize: [56, 24],
        iconAnchor: [28, 12],
        popupAnchor: [0, -12],
        html: `
          <div class="bus-marker">
            <div class="bus-pill" style="background:${color};">
              <div class="bus-arrow" style="transform: rotate(${rot}deg);">&#10148;</div>
              <span>${routeLabel}</span>
            </div>
          </div>
        `
      });
    }

    function popupContent(v) {
      const delayText = (v.delay_min != null)
        ? (v.anomaly ? `${v.delay_min > 0 ? '+' : ''}${v.delay_min} min (flagged as anomalous)` : `${v.delay_min > 0 ? '+' : ''}${v.delay_min} min`)
        : 'No current delay data';
      const speedKmh = (v.speed != null) ? Math.round(v.speed * 3.6) + ' km/h' : 'Speed unavailable';
      return `
        <strong>Route ${v.route_num || v.route_id || '?'}</strong><br>
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
        const icon = makeIcon(routeLabel, v.bearing, v.color);
        const popup = popupContent(v);

        if (markers.has(key)) {
          const m = markers.get(key);
          m.setLatLng([v.lat, v.lon]);
          m.setIcon(icon);
          m.getPopup().setContent(popup);
        } else {
          const m = L.marker([v.lat, v.lon], { icon }).addTo(map).bindPopup(popup);
          markers.set(key, m);
        }
      });

      for (const [key, m] of markers) {
        if (!seen.has(key)) {
          map.removeLayer(m);
          markers.delete(key);
        }
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
        map_error=map_error,
        color_on_time=COLOR_ON_TIME,
        color_late=COLOR_LATE,
        color_early=COLOR_EARLY,
        color_no_data=COLOR_NO_DATA,
    )


@app.route("/api/vehicles")
def api_vehicles():
    if not API_KEY:
        return jsonify({"error": "TFNSW_API_KEY not set in .env"}), 500
    data = compute_delay_data(request.args)
    vehicles, map_error = compute_vehicles(data)
    return jsonify({"vehicles": vehicles, "error": map_error})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)