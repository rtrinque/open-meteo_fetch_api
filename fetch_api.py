import argparse
import requests
from datetime import datetime
from collections import defaultdict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import unittest

def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def aggregate_hourly_data(hourly_data):
    aggregated_data = defaultdict(lambda: [0, 0, 0, 0])

    for time, values in zip(hourly_data['time'], zip(hourly_data['temperature_2m'], hourly_data['rain'], hourly_data['showers'], hourly_data['visibility'])):
        date_obj = datetime.strptime(time, '%Y-%m-%dT%H:%M').date()
        # Aggregate values by day
        aggregated_data[date_obj][0] += values[0]  # Temperature
        aggregated_data[date_obj][1] += values[1]  # Rain
        aggregated_data[date_obj][2] += values[2]  # Showers
        aggregated_data[date_obj][3] += values[3]  # Visibility

    return aggregated_data

def save_to_parquet(aggregated_data, output_file):
    try:
        # Create a DataFrame with separate columns for each field
        df = pd.DataFrame.from_dict(aggregated_data, orient='index', columns=['Temperature', 'Rain', 'Showers', 'Visibility'])
        df.index = pd.to_datetime(df.index)  # Convert the index to datetime
        df.index.name = 'Date'  # Add a label for the date index
        pq.write_table(pa.Table.from_pandas(df), output_file)
        print(f"Aggregated data saved as '{output_file}'.")
    except Exception as e:
        print(f"Error saving data to Parquet file: {e}")

class TestAggregation(unittest.TestCase):
    def test_aggregate_data(self):
        # Example API data for testing
        api_data = {
            "latitude": 51.5,
            "longitude": -0.120000124,
            "generationtime_ms": 45.50302028656006,
            "utc_offset_seconds": 0,
            "timezone": "GMT",
            "timezone_abbreviation": "GMT",
            "elevation": 23,
            "hourly_units": {
                "time": "iso8601",
                "temperature_2m": "°C",
                "rain": "mm",
                "showers": "mm",
                "visibility": "m",
            },
            "hourly": {
                "time": ["2022-07-01T00:00", "2022-07-01T01:00"],
                "temperature_2m": [13.7, 13.3],
                "rain": [82, 83],
                "showers": [82, 83],
                "visibility": [82, 83],
            },
        }

        hourly_data = api_data.get("hourly", {})
        aggregated_data = aggregate_hourly_data(hourly_data)

        # Test total for each date
        self.assertEqual(aggregated_data[datetime(2022, 7, 1).date()], [27.0, 165.0, 165.0, 165.0])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch hourly data from an API, aggregate by date, and save as Parquet file.')
    parser.add_argument('api_url', type=str, help='API endpoint URL')
    args = parser.parse_args()

    api_data = fetch_data_from_api(args.api_url)

    if api_data is not None:
        hourly_data = api_data.get("hourly", {})
        aggregated_data = aggregate_hourly_data(hourly_data)
        save_to_parquet(aggregated_data, 'aggregated_hourly_data.parquet')
    else:
        print("Exiting due to API error.")

    # To be finished
    # Run unit tests
    # unittest.main()

