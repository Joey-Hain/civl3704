# Application Architecture

The app is organised into three main layers:

## 1. Data layer

- Loads configuration and API credentials from `.env`.
- Fetches live trip updates from the TfNSW GTFS-realtime feed.
- Fetches live vehicle positions from the TfNSW vehicle-position feed.
- Downloads static GTFS schedule data for operator names and trip headsigns.
- Caches successfully loaded schedule data for the lifetime of the process and retries failed loads after a backoff period.
- Uses short-lived, lock-protected in-memory caches to reduce repeated API requests and prevent concurrent duplicate fetches.
- Fetches historical daily CSV data from the `gtfs-r-scrape` repository for the heatmap.
- Records trip-update readings in `CIVL3704/delay_log.csv`.

## 2. Processing layer

- Parses GTFS-realtime protobuf feeds.
- Extracts trip delays, routes, stops, operators and vehicle locations.
- Identifies anomalous delay readings.
- Selects the latest reading for each trip.
- Calculates delay statistics by operator and route.
- Joins vehicle positions with delays using `trip_id`.
- Applies route, stop, operator, anomaly and geographic-bound filters.
- Aggregates historical readings into geographic grid cells.
- Calculates historical heatmap weights for delay, vehicle density and speed, including confidence weighting for mean-based metrics.
- Prepares vehicle data for both the dashboard and the `/api/vehicles` endpoint.

## 3. Visualisation layer

- Flask serves the dashboard, `/api/vehicles`, `/api/heatmap`, `/status`, `/health` and `/ping` endpoints.
- Jinja renders summary tables for operators, routes and individual trips.
- Leaflet displays live vehicle markers on an interactive map.
- Marker outlines show whether a vehicle is on time, late, early or has no delay data.
- Leaflet.heat displays live and historical heatmaps for delay, vehicle density and speed.
- Historical heatmaps support one-hour, 24-hour and seven-day windows and refresh automatically every five minutes.
- JavaScript polls vehicle data every 15 seconds without reloading the tables.
- The `/project` view provides a non-interactive map locked to the smart-city physical model's geographic bounds.
- HTML, CSS and JavaScript provide the dashboard layout, filters, layer controls, legends, popups and loading/error states.
