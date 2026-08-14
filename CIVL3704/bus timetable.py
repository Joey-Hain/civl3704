"""
Pull delay/variance data out of a TfNSW GTFS-realtime feed and summarise it.

Each stop_time_update in a TripUpdate can carry a `delay` field (seconds,
positive = late, negative = early) computed by TfNSW against the static
timetable. This script collects those, saves them to CSV so you can build
up a history over multiple runs, and prints summary stats.

Setup:
    pip install requests python-dotenv gtfs-realtime-bindings tzdata

.env file:
    TFNSW_API_KEY=your_api_key_here
    TFNSW_url=https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses

Usage:
    python timetable_variance.py                 # all routes
    python timetable_variance.py 2504             # only routes containing "2504"
"""

import csv
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
LOG_FILE = Path("CIVL3704/delay_log.csv")

load_dotenv()
API_KEY = os.getenv("TFNSW_API_KEY")
FEED_URL = os.getenv("TFNSW_GTFS_RT_URL", "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses")


def fetch_feed(url: str, api_key: str) -> gtfs_realtime_pb2.FeedMessage:
    headers = {"Authorization": f"apikey {api_key}"}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def extract_delays(feed: gtfs_realtime_pb2.FeedMessage, route_filter: str = None) -> list[dict]:
    """Pull one row per stop_time_update that has delay info."""
    rows = []
    pulled_at = datetime.now(tz=SYDNEY_TZ).isoformat()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip

        if route_filter and route_filter not in trip.route_id:
            continue

        for stu in tu.stop_time_update:
            arr_delay = stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None
            dep_delay = stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None

            if arr_delay is None and dep_delay is None:
                continue  # nothing to log for this stop

            rows.append({
                "pulled_at": pulled_at,
                "route_id": trip.route_id,
                "trip_id": trip.trip_id,
                "start_date": trip.start_date,
                "stop_id": stu.stop_id,
                "stop_sequence": stu.stop_sequence,
                "arrival_delay_sec": arr_delay,
                "departure_delay_sec": dep_delay,
            })

    return rows


def append_to_log(rows: list[dict]) -> None:
    file_exists = LOG_FILE.exists()
    with LOG_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "pulled_at", "route_id", "trip_id", "start_date",
            "stop_id", "stop_sequence", "arrival_delay_sec", "departure_delay_sec",
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict]) -> None:
    if not rows:
        print("No delay data found in this pull (feed may not populate `delay` for this mode — "
              "try comparing arrival/departure times against a static schedule instead).")
        return

    by_route = defaultdict(list)
    for r in rows:
        d = r["arrival_delay_sec"] if r["arrival_delay_sec"] is not None else r["departure_delay_sec"]
        if d is not None:
            by_route[r["route_id"]].append(d)

    print(f"{'Route':<15} {'n':>4} {'mean(s)':>9} {'stdev(s)':>9} {'min':>6} {'max':>6}")
    print("-" * 55)
    for route_id, delays in sorted(by_route.items(), key=lambda kv: -statistics.mean(kv[1])):
        mean = statistics.mean(delays)
        stdev = statistics.stdev(delays) if len(delays) > 1 else 0.0
        print(f"{route_id:<15} {len(delays):>4} {mean:>9.1f} {stdev:>9.1f} {min(delays):>6} {max(delays):>6}")

    all_delays = [d for v in by_route.values() for d in v]
    print("-" * 55)
    print(f"Overall: n={len(all_delays)}  mean={statistics.mean(all_delays):.1f}s  "
          f"stdev={statistics.stdev(all_delays):.1f}s" if len(all_delays) > 1 else "")


def main() -> None:
    if not API_KEY:
        sys.exit("TFNSW_API_KEY not set — check your .env file.")

    route_filter = sys.argv[1] if len(sys.argv) > 1 else None

    try:
        feed = fetch_feed(FEED_URL, API_KEY)
    except requests.exceptions.RequestException as e:
        sys.exit(f"Request failed: {e}")

    rows = extract_delays(feed, route_filter=route_filter)
    append_to_log(rows)
    print(f"Logged {len(rows)} stop-level delay readings to {LOG_FILE.resolve()}\n")
    print_summary(rows)


if __name__ == "__main__":
    main()