# TfNSW Delay Board

A Flask dashboard that displays live TfNSW bus delays, vehicle locations, route statistics, and vehicle density.

## Setup

Install the dependencies:

```bash
pip install flask requests python-dotenv gtfs-realtime-bindings tzdata
```

Create a `.env` file in the project directory:

```env
TFNSW_API_KEY=your_api_key_here
TFNSW_GTFS_RT_URL=https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses
```

A TfNSW API key with access to the required realtime and schedule feeds is required.

## Run

```bash
python app.py
```

Open [http://localhost:5000](http://localhost:5000) in a browser.

## Features

- Live bus delay information
- Operator and route summaries
- Interactive vehicle map
- Delay-status marker colours
- Vehicle-density heatmap
- Route, stop, operator, and anomaly filters

The dashboard automatically refreshes vehicle positions every 15 seconds.


## Visualisations

- **Live map:** Each marker represents a currently reporting bus. The label shows its route, and the arrow shows its direction of travel. Marker outlines indicate the bus status: white for on time (0:59 early to 5:59 late, matching the TfNSW on-time KPI), red for late, green for early, and grey when delay data is unavailable or anomalous.
- **Heatmap metric:** Use the layer control and metric selector to display live or historical patterns for mean delay, median delay, delay standard deviation, % of readings not on time, vehicle density, or speed. Delay, not-on-time % and speed are drawn as an averaged field: colour is the local mean of nearby readings (so busy streets don't look worse just for having more readings) and opacity shows how much data backs it. Density is drawn as a classic additive heatmap. Speed uses a 0–80 km/h scale (amber = CBD streets, pink = arterials, purple = motorways). Delay distribution metrics apply to historical cells; the live layer shows current per-vehicle lateness.
- **Route clipping:** "Clip to bus routes" trims both heat layers to within ±15–100 m of the bus road network (TfNSW GTFS route shapes, rebuilt weekly in gtfs-r-scrape), so delay reads along corridors instead of as round blobs. Set it to Off for the unclipped view.
- **Historical heatmap:** Historical data can be viewed over the last hour, 24 hours, or 7 days, and filtered to a time of day (AM peak 06:00–10:00, midday 10:00–15:00, PM peak 15:00–19:00, evening 19:00–06:00). The map aggregates vehicle readings into geographic cells, so each coloured area represents activity within a small location rather than a single bus.
- **Operator and route tables:** These show the number of distinct bus readings, average delay, variation in delay, percentage running on time, and the observed delay range. Routes can be sorted by delay, spread, or on-time percentage.
- **Individual trips table:** This lists the trips with the largest current delays and identifies their route, operator, most recent stop, and delay status.
