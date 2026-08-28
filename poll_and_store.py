import os
import sys
import requests
import psycopg2
from psycopg2.extras import execute_values
from google.transit import gtfs_realtime_pb2

# 1. Fetch Environment Variables from GitHub Actions Secrets
TFNSW_API_KEY = os.environ.get("TFNSW_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not TFNSW_API_KEY or not DATABASE_URL:
    print("Error: Missing TFNSW_API_KEY or DATABASE_URL environment variables.")
    sys.exit(1)

def setup_database(conn):
    """Creates the target table and optimization index in Supabase if they do not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tfnsw_bus_positions (
                id SERIAL PRIMARY KEY,
                vehicle_id VARCHAR(100),
                trip_id VARCHAR(100),
                route_id VARCHAR(100),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                bearing REAL,
                speed REAL,
                fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_tfnsw_fetched_at ON tfnsw_bus_positions (fetched_at DESC);
        """)
        conn.commit()
    print("Database table schema verified/created.")

def fetch_tfnsw_data():
    """Fetches real-time binary Protocol Buffer data from the TfNSW API."""
    # Defaulting to standard TfNSW GTFS-R Bus Positions
    url = "https://nsw.gov.au"
    headers = {
        "Authorization": f"apikey {TFNSW_API_KEY}",
        "Accept": "application/x-google-protobuf"
    }
    
    print("📡 Fetching real-time records from TfNSW API...")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"API Request failed: {e}")
        return None

def parse_and_store(pb_data, conn):
    """Parses the binary GTFS-R Protocol Buffer data and batch-inserts it into Supabase."""
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(pb_data)
    except Exception as e:
        print(f"Failed to parse Protocol Buffer: {e}")
        return

    data_to_insert = []
    
    # Loop through the GTFS Realtime entities
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            # Extract attributes safely with fallbacks
            vehicle_id = v.vehicle.id if v.vehicle.id else None
            trip_id = v.trip.trip_id if v.trip.trip_id else None
            route_id = v.trip.route_id if v.trip.route_id else None
            lat = v.position.latitude
            lon = v.position.longitude
            bearing = v.position.bearing if v.position.HasField('bearing') else None
            speed = v.position.speed if v.position.HasField('speed') else None
            
            data_to_insert.append((vehicle_id, trip_id, route_id, lat, lon, bearing, speed))

    if not data_to_insert:
        print("No valid vehicle position records found in this tick.")
        return

    # High-efficiency batch insert using psycopg2 helpers
    query = """
        INSERT INTO tfnsw_bus_positions (vehicle_id, trip_id, route_id, latitude, longitude, bearing, speed)
        VALUES %s
    """
    
    print(f"Writing {len(data_to_insert)} records to Supabase...")
    with conn.cursor() as cur:
        execute_values(cur, query, data_to_insert)
        conn.commit()
    print("Batch ingestion complete.")

def main():
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)
        
    try:
        setup_database(conn)
        raw_payload = fetch_tfnsw_data()
        if raw_payload:
            parse_and_store(raw_payload, conn)
    finally:
        conn.close()
        print("Database connection closed cleanly.")

if __name__ == "__main__":
    main()
