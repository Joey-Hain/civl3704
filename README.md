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
