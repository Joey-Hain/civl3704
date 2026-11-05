# TfNSW Delay Board

The TfNSW Delay Board combines live bus locations and delay predictions with
historical service data. Flask serves the interactive dashboard and JSON API;
the companion Raspberry Pi Pico firmware lets a presenter control map layers
with physical buttons.

## Application API

The `app` module contains the Flask application, data-processing helpers, and
HTTP endpoints. The reference below is generated from the Python docstrings.

::: app
    options:
      members:
        - app
        - time_of_day_bucket
        - is_on_time
        - haversine_km
        - parse_ts
        - get_schedule_lookups
        - fetch_feed
        - fetch_vehicle_feed
        - extract_rows
        - latest_reading_per_trip
        - extract_vehicles
        - merge_vehicle_delays
        - summarise
        - split_route
        - get_all_rows_cached
        - get_vehicles_cached
        - CellGrid
        - fetch_historical_cells
        - cells_from_prebuilt
        - compute_metric_points
        - metric_prior
        - WindowSpec
        - parse_window_args
        - get_historical_cells_cached
        - get_heatmap_points_cached
        - compute_delay_data
        - compute_vehicles
        - render_map_script
        - project
        - ping
        - health
        - status
        - dashboard
        - api_vehicles
        - api_route_shapes
        - compute_trip_detail
        - api_trip
        - api_heatmap
        - api_validation

## Pico button firmware

Copy `main.py` from the project root to a Raspberry Pi Pico. The firmware uses
internal pull-downs, so wire normally-open momentary buttons between GPIO 15,
14, or 13 and the Pico's 3V3 pin. The firmware sends the key on a
falling edge, so with this wiring the action occurs when a button is released.
If the USB keyboard is already open when the Pico starts, the firmware may
also send each key once during startup. The buttons send:

| Button | Dashboard action |
|---|---|
| GPIO 15 (A) | Cycle the heatmap layer |
| GPIO 14 (B) | Cycle the heatmap metric |
| GPIO 13 (C) | Toggle bus markers |

`tap(key)` sends a key press followed by a release to prevent key repeat. The
firmware runs continuously on MicroPython and depends on hardware modules, so
MkDocs includes its source instead of importing it:

```python
--8<-- "main.py"
```

## Further reading

- [Application architecture](ARCHITECTURE.md)
