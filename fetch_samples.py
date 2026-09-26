import os
import json
import urllib.request
from pathlib import Path

# Create directories
base_dir = Path(r"d:\Semester Projects\UrbanLake")
full_load_dir = base_dir / "data" / "full_load"
incremental_load_dir = base_dir / "data" / "incremental_load"

full_load_dir.mkdir(parents=True, exist_ok=True)
incremental_load_dir.mkdir(parents=True, exist_ok=True)

def fetch_and_save(url, filename):
    print(f"Fetching data for {filename.name}...")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"Success: Saved {filename.name}")
    except Exception as e:
        print(f"Error fetching {filename.name}: {e}")

# 1. TfL Traffic Sample (All major corridors, historical date range mock)
import random
from datetime import datetime, timedelta

tfl_road_ids = ["a1","a10","a12","a13","a2","a20","a205","a21","a23","a24","a3","a316","a4","a40","a406","a41","bishopsgate-cross-route","blackwall-tunnel","city-route","farringdon-cross-route","inner-ring","silvertown-tunnel","southern-river-route","western-cross-route"]
severities = ["Good", "Good", "Good", "Minor Delays", "Severe Delays", "Closure"]

traffic_results = []
base_time = datetime(2024, 1, 1, 0, 0, 0)

for road_id in tfl_road_ids:
    for day in range(7):
        for hour in range(24):
            current = base_time + timedelta(days=day, hours=hour)
            status = random.choice(severities)
            desc = "No Exceptional Delays" if status == "Good" else f"Traffic incident causing {status}"
            traffic_results.append({
                "$type": "Tfl.Api.Presentation.Entities.RoadCorridor, Tfl.Api.Presentation.Entities",
                "id": road_id,
                "displayName": road_id.upper().replace("-", " "),
                "statusSeverity": status,
                "statusSeverityDescription": desc,
                "statusAggregationStartDate": current.strftime("%Y-%m-%dT%H:00:00Z"),
                "statusAggregationEndDate": (current + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00Z"),
                "bounds": "[[-0.256,51.531],[-0.102,51.656]]",
                "url": f"/Road/{road_id}"
            })

with open(full_load_dir / "traffic_full_sample.json", "w", encoding="utf-8") as f:
    json.dump(traffic_results, f, indent=4)
print("Success: Saved traffic_full_sample.json (Expanded Mock Data - 4000+ records)")

# 2. OpenAQ Air Quality Sample (London PM2.5 measurements)
import random
from datetime import datetime, timedelta

sensors = [
    {"id": 2490, "name": "London Harlington", "lat": 51.48879, "lon": -0.441614},
    {"id": 2491, "name": "London Marylebone Road", "lat": 51.52253, "lon": -0.154611},
    {"id": 2492, "name": "London N. Kensington", "lat": 51.52105, "lon": -0.213492},
    {"id": 2493, "name": "London Bloomsbury", "lat": 51.52229, "lon": -0.125889}
]

start_time = datetime(2024, 1, 1, 0, 0, 0)
results = []

for sensor in sensors:
    for hour in range(24): # 24 hours of data per sensor
        current_time = start_time + timedelta(hours=hour)
        results.append({
            "locationId": sensor["id"],
            "location": sensor["name"],
            "parameter": "pm25",
            "value": round(random.uniform(5.0, 35.0), 1),
            "date": {
                "utc": current_time.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                "local": current_time.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            },
            "unit": "µg/m³",
            "coordinates": {"latitude": sensor["lat"], "longitude": sensor["lon"]},
            "country": "GB",
            "city": "London"
        })

openaq_mock_data = {
    "meta": {"name": "openaq-api", "license": "CC BY 4.0", "website": "https://openaq.org", "found": len(results)},
    "results": results
}

with open(full_load_dir / "air_quality_full_sample.json", "w", encoding="utf-8") as f:
    json.dump(openaq_mock_data, f, indent=4)
print("Success: Saved air_quality_full_sample.json (Expanded Mock Data)")

# 3. Open-Meteo Weather Sample (Historical London data)
meteo_url = "https://archive-api.open-meteo.com/v1/archive?latitude=51.5085&longitude=-0.1257&start_date=2024-01-01&end_date=2024-01-02&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,precipitation"
fetch_and_save(meteo_url, full_load_dir / "weather_full_sample.json")

print("\nPhase 1 Data Samples successfully generated!")
