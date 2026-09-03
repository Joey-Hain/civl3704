# Application Architecture

The app is organised into three main layers:

## 1. Data layer

- Loads configuration and API credentials or accesse credentials from `.env`.
- Fetches live trip updates from the TfNSW GTFS-realtime feed.
- Fetches live vehicle positions from the TfNSW vehicle-position feed.
- Downloads static GTFS schedule data for operator names and trip headsigns.
- Caches schedule data for 24 hours.
- Uses short-lived in-memory caches to reduce repeated API requests.
- Records trip-update readings in `CIVL3704/delay_log.csv`.

## 2. Processing layer

- Parses GTFS-realtime protobuf feeds.
- Extracts trip delays, routes, stops, operators and vehicle locations.
- Identifies anomalous delay readings.
- Selects the latest reading for each trip.
- Calculates delay statistics by operator and route.
- Joins vehicle positions with delays using `trip_id`.
- Applies route, stop, operator, anomaly and geographic-bound filters.
- Prepares vehicle data for both the dashboard and the API.

## 3. Visualisation layer

- Flask serves the dashboard and `/api/vehicles` endpoint.
- Jinja renders summary tables for operators, routes and individual trips.
- Leaflet displays live vehicle markers on an interactive map.
- Marker outlines show whether a vehicle is on time, late, early or has no delay data.
- Leaflet.heat displays the current vehicle-density heatmap.
- JavaScript polls vehicle data every 15 seconds without reloading the tables.
- HTML and CSS provide the dashboard layout, filters, legends and popup styling.
