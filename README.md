# TfNSW Delay Board

A Flask dashboard that turns live TfNSW bus data into delay statistics, a live vehicle map and live and historical heatmaps of delay, on-time running, speed and bus density. A projector mode locks the map to the TransportLab physical model of Sydney so the heatmaps can be projected onto it.

Built for CIVL3704 Transport Informatics (University of Sydney) by Group 1. The deployed dashboard is at [civl3704.joeyhain.org](https://civl3704.joeyhain.org) and projector mode at [/project](https://civl3704.joeyhain.org/project).

## Setup

Install the dependencies (Python 3.11):

```bash
pip install -r requirements.txt
```

Create a `.env` file in the project directory:

```env
TFNSW_API_KEY=your_api_key_here
TFNSW_GTFS_RT_URL=https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses
```

A TfNSW Open Data API key with access to the bus realtime trip update, vehicle position and timetable feeds is required. Never hardcode the key into a script; read it with `os.getenv("TFNSW_API_KEY")`.

## Run

```bash
python app.py
```

Open [http://localhost:5000](http://localhost:5000) in a browser. The first page load after starting takes about 30 seconds while the bus timetable is downloaded; after that pages load immediately.

## Features

- Live bus delay information, with on time defined by the TfNSW on-time running KPI (no more than 59 seconds early or 5 minutes 59 seconds late)
- Operator and route summaries
- Interactive vehicle map with delay-status marker colours
- Live and historical heatmaps: average delay, delay burden, not on time, speed and bus density
- Historical windows from the last hour to 30 days, custom date ranges and time-of-day filters
- Heatmaps clipped to the bus road network so delay reads along corridors
- Projector mode with corner calibration for the TransportLab model
- Operator dropdown (operators sharing a name are grouped) plus route and stop ID filters

The dashboard automatically refreshes vehicle positions every 15 seconds and the historical heatmap every 5 minutes.

## Visualisations

- **Live map:** Each marker represents a currently reporting bus. The label shows its route, and the arrow shows its direction of travel. Marker outlines indicate the bus status: white for on time (0:59 early to 5:59 late, matching the TfNSW on-time KPI), red for late, green for early, and grey when delay data is unavailable or anomalous.
- **Heatmap metric:** The Heatmap section of the map menu offers five metrics. *Average delay* (0–5 min scale) and *Not on time* (share of readings outside the TfNSW on-time window, 0–60% scale) are drawn as an averaged field: colour is the local mean of readings within 250 m, shrunk toward the network average where data is thin so a single late bus can't paint a hotspot, and opacity shows how much data backs it. *Speed* (0–80 km/h; amber = CBD streets, pink = arterials, purple = motorways) is averaged the same way over 160 m. *Delay burden* (total bus-minutes late) and *Bus density* are additive heatmaps, so busy corridors rank high by design. The live layer shows each bus's current value. "About the heatmaps" under the map explains each metric.
- **Route clipping:** "Route clip" trims both heat layers to within ±15–100 m of the bus road network (TfNSW GTFS route shapes, rebuilt weekly in gtfs-r-scrape), so delay reads along corridors instead of as round blobs. Set it to Off for the unclipped view.
- **Historical heatmap:** Historical data can be viewed over the last hour, 24 hours, 7 days, 30 days (the default — the GitHub scraper only gets 3–6 snapshots a day, so shorter windows are sparse) or a custom date range of up to 62 days, and filtered to a time of day (AM peak 06:00–10:00, midday 10:00–15:00, PM peak 15:00–19:00, evening 19:00–06:00). The map aggregates vehicle readings into geographic cells, so each coloured area represents activity within a small location rather than a single bus.
- **Projector mode:** `/project` locks the map to the TransportLab model's extent and blacks out everything outside it. "Hide menu" (or the M key, or `?menu=0`) removes the menu so only the map is projected.
- **Operator and route tables:** These show the number of distinct bus readings, average delay, variation in delay, percentage running on time, and the observed delay range. Routes can be sorted by delay, spread, or on-time percentage.
- **Individual trips table:** This lists the trips with the largest current delays and identifies their route, operator, most recent stop, and delay status.

## Projector calibration

1. Open `/project` on the computer connected to the projector, full screen (F11) at the projector's native resolution.
2. Press C (or "Calibrate projection" in the menu). A crosshair appears on each corner of the model's area.
3. Drag each crosshair onto the matching corner of the physical model. Tab selects the next crosshair and the arrow keys nudge it by 1 px (Shift for 10 px). Align to street-level corners rather than building tops, as the buildings stand above the table.
4. Press C again to finish. The map is warped so the model's corners land on the crosshairs, and nothing is projected outside them.

The calibration is saved in that browser. The calibration box also shows a link containing it; bookmark that link on the projector computer so the calibration can be restored anywhere. R resets it.

## Data

- **Live data:** TfNSW GTFS-Realtime bus trip updates (delays) and vehicle positions, joined on trip ID. The static GTFS timetable supplies operator names and trip destinations.
- **Historical data:** Vehicle positions with delays are collected by the separate [gtfs-r-scrape](https://github.com/Joey-Hain/gtfs-r-scrape) repository into daily CSV files (`data/` from a GitHub Action, `data-local/` from a home machine running `local_collector.py`). The dashboard reads both for each day in the selected window.
- **Route shapes:** gtfs-r-scrape also builds `shapes/route_shapes.json`, a de-duplicated bus road network within 12 km of the CBD, used for route clipping.

## API

All endpoints return JSON unless noted.

| Endpoint | Description |
|---|---|
| `/` | Dashboard page. Query parameters `operator`, `route`, `stop`, `bounds=0` (whole Opal area) and `sort`. |
| `/project` | Projector mode. `menu=0` hides the menu; `cal=...` restores a calibration. |
| `/api/vehicles` | Live vehicle positions with delay and status. Accepts the same filters as `/`. |
| `/api/heatmap` | Historical heatmap points. `window=1`, `24`, `168` or `720` (hours), or `window=custom&start=YYYY-MM-DD&end=YYYY-MM-DD`; `metric=delay`, `delay_total`, `frequency`, `speed` or `density`; `period=all`, `am_peak`, `midday`, `pm_peak` or `evening`. |
| `/api/validation` | On-time, early and late percentages and mean lateness by operator from the historical data, for comparison with TfNSW's published on-time running. Same `window` parameters; `format=csv` for a spreadsheet. |
| `/api/route_shapes` | The bus road network used for route clipping. |
| `/status` | Cache and timetable diagnostics. |
| `/ping` | Health check; returns `pong`. |

## Deployment

The live site runs on Render.com from this repository's `main` branch, redeploying automatically on every push. `render.yaml` holds the settings (start command `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 6 --worker-class gthread --timeout 120`, health check `/ping`), and `TFNSW_API_KEY` and `TFNSW_GTFS_RT_URL` are set as environment variables on Render. Don't change `Procfile`, `requirements.txt` or `render.yaml` without checking they still deploy. The free instance has 512 MB of memory and sleeps after 15 minutes without a request; `local_collector.py` in gtfs-r-scrape pings the site every 10 minutes to keep it awake.

To run your own copy, create a Render web service from a fork of this repository (it picks up `render.yaml`) and add your API key as `TFNSW_API_KEY`.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): how the data pipeline, heatmap aggregation and front end fit together
- The docstring at the top of `app.py` explains the caching, memory limits and heatmap methodology in detail
