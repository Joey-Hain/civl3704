# Application Architecture

The app is organised into three main layers:

## 1. Data layer

- Loads configuration and API credentials from `.env`.
- Fetches live trip updates from the TfNSW GTFS-realtime feed.
- Fetches live vehicle positions from the TfNSW vehicle-position feed.
- Downloads static GTFS schedule data for operator names and trip headsigns.
- Caches successfully loaded schedule data for the lifetime of the process and retries failed loads after a backoff period.
- Uses short-lived, lock-protected in-memory caches to reduce repeated API requests and prevent concurrent duplicate fetches.
- Reads the historical heatmap from pre-aggregated grid cells that the `gtfs-r-scrape` Action builds after every data push (`build_cells.py`, published to that repository's `cells` branch): one file per preset window (last 1, 24 and 168 hours of collected data, ending at the newest snapshot) and one per Sydney day, merged on the server for custom ranges of up to seven days. A preset window is a single conditional download (usually HTTP 304) and a few `array.frombytes` calls, so Render's small free-tier CPU never parses CSVs. Aggregating on GitHub's runners was needed once 5-minute collection made a week ~640k rows, more than the Render instance could parse within the 20-second heatmap deadline. If the `cells` branch is unavailable the server falls back to downloading and parsing the daily CSVs (`data/` and `data-local/`) itself, with a gzipped disk cache of finished days, one retry on HTTP 429/5xx, and an optional `GITHUB_TOKEN` for authenticated downloads.
- Heatmap responses are rounded, gzipped once per window build and served with an ETag, so a dashboard polling every five minutes gets HTTP 304 with no body until the data changes; other large responses (the page itself, `/api/vehicles`) are gzipped too.
- For a clicked bus, fetches only the matching shards of gtfs-r-scrape's per-trip lookup (`shapes/trips/NNN.json` for trip → shape, `shapes/geom/NNN.json` for the shape, shard = `zlib.crc32(id) % 128`) plus `shapes/stops.json` once, held in a bounded LRU cache so the full timetable never loads into the 512 MB instance.

## 2. Processing layer

- Parses GTFS-realtime protobuf feeds.
- Extracts trip delays, routes, stops, operators and vehicle locations.
- Identifies anomalous delay readings.
- Selects the latest reading for each trip.
- Calculates delay statistics by operator and route.
- Joins vehicle positions with delays using `trip_id`.
- Applies route, stop, operator, anomaly and geographic-bound filters.
- Aggregates historical readings into geographic grid cells.
- Calculates historical heatmap values for average delay, delay burden (total lateness), % not on time (TfNSW KPI window, -59s to +5:59), speed and bus density. Average delay and % not on time are sent as raw per-cell values weighted by reading count, plus a network prior (`metric_prior`) the client uses for per-pixel empirical-Bayes shrinkage (`SHRINK_PRIOR_N`). Median and SD were tried and removed: at ~1.3 readings per cell they add nothing over the mean.
- Prepares vehicle data for both the dashboard and the `/api/vehicles` endpoint.
- Builds a selected trip's detail (`/api/trip/<trip_id>`): every remaining stop-time update for the trip from the cached live feed, in stop order with predicted delays, joined to stop names and locations and to the trip's timetable shape.

## 3. Visualisation layer

- Flask serves the dashboard, `/api/vehicles`, `/api/heatmap` (`window=1|24|168` or `window=custom&start=&end=`), `/api/validation` (on-time running by operator from the scraped history, JSON or `&format=csv`, for comparison with TfNSW's published results), `/api/route_shapes`, `/api/trip/<trip_id>`, `/status`, `/health` and `/ping` endpoints.
- Jinja renders summary tables for operators, routes and individual trips.
- Leaflet displays live vehicle markers on an interactive map.
- Marker outlines show whether a vehicle is on time, late, early or has no delay data.
- Clicking a marker draws that trip's route in its own pane between the heat layers and the markers, split at the vehicle's nearest shape vertex (dashed behind, solid with a white casing ahead), with upcoming stops coloured by predicted delay against the on-time window. The popup summarises the next stop, the last predicted stop and the expected change in delay; it refreshes with each vehicle poll and clears when the popup closes.
- Value metrics (average delay, % not on time, speed) render through a custom `FieldLayer`: a confidence- and Gaussian-weighted mean per pixel (normalised convolution) with opacity from data support, coloured with perceptually ordered ramps interpolated in OKLab. Density stays on Leaflet.heat, whose additive stacking is correct only for counts.
- Leaflet.heat displays the additive metrics (delay burden, bus density).
- An optional route mask clips both heat layers to the bus road network: `gtfs-r-scrape/build_route_shapes.py` (weekly GitHub Action) de-duplicates TfNSW GTFS `shapes.txt` within 12 km of the CBD into `shapes/route_shapes.json`; the app serves it gzipped from `/api/route_shapes` and the client strokes it with a `destination-in` composite after each heat redraw.
- Historical heatmaps support one-hour, 24-hour and seven-day (default) windows, plus custom ranges of up to seven days (a 30-day window was dropped once 5-minute collection made seven days dense enough and 30 days too slow to parse on Render's free CPU) — daily CSVs are downloaded in parallel and streamed serially into one set of cells. Cells keep running sums and counts only (including an off-time count for % not on time), never individual readings, and a finished window is frozen into flat `array('d')` columns, so memory grows with the number of 10 m cells rather than the number of readings (24 h + 7 day + 30 day windows ≈ 150 MB on 16 Oct data, against ~570 MB before), a time-of-day filter (AM peak, midday, PM peak, evening) bucketed during the same aggregation pass, and refresh automatically every five minutes.
- JavaScript polls vehicle data every 15 seconds without reloading the tables.
- The `/project` view provides a non-interactive map locked to the TransportLab physical model's geographic bounds ("Projector mode" in the UI).
- Projector geometry: by default the model box is laid out as smart-city's `params.json5` does (full-width box at the model's 2:1 aspect, `vertical_offset` 50 px, `horizontal_scale` 1.184, shrunk uniformly if needed so all four corners stay on screen), computed from unrounded Web Mercator positions and applied through the same warp below.
- Projector calibration: a corner-pin warp (homography applied as a CSS `matrix3d` to the map) maps the model's four corners onto four draggable crosshairs, with an SVG mask blacking out everything outside them; saved per browser and shareable as a `?cal=` link.
- HTML, CSS and JavaScript provide the dashboard layout, filters, layer controls, legends, popups and loading/error states.
- The shared map script listens for USB HID keyboard keys A, B and C from `pico_buttons.py`: A cycles live/historical/no heatmap, B cycles all heatmap metrics, and C toggles bus-marker visibility.
