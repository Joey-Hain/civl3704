# Application Architecture

The app is organised into three main layers:

## 1. Data layer

- Loads configuration and API credentials from `.env`.
- Fetches live trip updates from the TfNSW GTFS-realtime feed.
- Fetches live vehicle positions from the TfNSW vehicle-position feed.
- Downloads static GTFS schedule data for operator names and trip headsigns.
- Caches successfully loaded schedule data for the lifetime of the process and retries failed loads after a backoff period.
- Uses short-lived, lock-protected in-memory caches to reduce repeated API requests and prevent concurrent duplicate fetches.
- Fetches historical daily CSV data from the `gtfs-r-scrape` repository for the heatmap: `data/` (GitHub Actions scrape, 3–6 snapshots a day) and `data-local/` (same format, from `local_collector.py` on a home machine every 15 minutes, pushed manually). Both are read for every day in the window.
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
- Calculates historical heatmap values for average delay, delay burden (total lateness), % not on time (TfNSW KPI window, -59s to +5:59), speed and bus density. Average delay and % not on time are sent as raw per-cell values weighted by reading count, plus a network prior (`metric_prior`) the client uses for per-pixel empirical-Bayes shrinkage (`SHRINK_PRIOR_N`). Median and SD are still accepted by `/api/heatmap` but no longer offered in the UI (at ~1.3 readings per cell they add nothing over the mean).
- Prepares vehicle data for both the dashboard and the `/api/vehicles` endpoint.

## 3. Visualisation layer

- Flask serves the dashboard, `/api/vehicles`, `/api/heatmap` (`window=1|24|168|720` or `window=custom&start=&end=`), `/api/validation` (on-time running by operator from the scraped history, JSON or `&format=csv`, for comparison with TfNSW's published results), `/api/route_shapes`, `/status`, `/health` and `/ping` endpoints.
- Jinja renders summary tables for operators, routes and individual trips.
- Leaflet displays live vehicle markers on an interactive map.
- Marker outlines show whether a vehicle is on time, late, early or has no delay data.
- Value metrics (average delay, % not on time, speed) render through a custom `FieldLayer`: a confidence- and Gaussian-weighted mean per pixel (normalised convolution) with opacity from data support, coloured with perceptually ordered ramps interpolated in OKLab. Density stays on Leaflet.heat, whose additive stacking is correct only for counts.
- Leaflet.heat displays the additive metrics (delay burden, bus density).
- An optional route mask clips both heat layers to the bus road network: `gtfs-r-scrape/build_route_shapes.py` (weekly GitHub Action) de-duplicates TfNSW GTFS `shapes.txt` within 12 km of the CBD into `shapes/route_shapes.json`; the app serves it gzipped from `/api/route_shapes` and the client strokes it with a `destination-in` composite after each heat redraw.
- Historical heatmaps support one-hour, 24-hour, seven-day and 30-day (default) windows — daily CSVs are downloaded in parallel and parsed serially into one set of cells (~3 s for 30 days), a time-of-day filter (AM peak, midday, PM peak, evening) bucketed during the same aggregation pass, and refresh automatically every five minutes.
- JavaScript polls vehicle data every 15 seconds without reloading the tables.
- The `/project` view provides a non-interactive map locked to the TransportLab physical model's geographic bounds ("Projector mode" in the UI).
- HTML, CSS and JavaScript provide the dashboard layout, filters, layer controls, legends, popups and loading/error states.
