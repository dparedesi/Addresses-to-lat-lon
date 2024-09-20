import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import logging
from tqdm import tqdm
import json
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add command-line arguments
parser = argparse.ArgumentParser(description='Geocode addresses from a CSV file.')
parser.add_argument('--input', default='inputs-outputs/input_locations.csv', help='Input CSV file path')
parser.add_argument('--output', default='inputs-outputs/output_lat_lon.csv', help='Output CSV file path')
parser.add_argument('--cache', default='geocode_cache.json', help='Cache file path')
parser.add_argument('--delay', type=float, default=1.0, help='Delay between geocoding requests in seconds')
args = parser.parse_args()

# Add a simple cache
geocode_cache = {}

def load_cache():
    try:
        with open(args.cache, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_cache():
    with open(args.cache, 'w') as f:
        json.dump(geocode_cache, f)

# Load the cache at the start
geocode_cache = load_cache()

def extract_postcode(address):
    pattern = r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b'
    match = re.search(pattern, address)
    return match.group(0) if match else None

def geocode(location, is_postcode=True):
    if location in geocode_cache:
        return geocode_cache[location]

    geolocator = Nominatim(user_agent="my_app")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if is_postcode:
                query = f"{location}, UK"
            else:
                query = location
            location_result = geolocator.geocode(query)
            if location_result:
                result = (location_result.latitude, location_result.longitude)
            else:
                result = (None, None)
            
            # Cache the result
            geocode_cache[location] = result
            return result
        except (GeocoderTimedOut, GeocoderServiceError):
            if attempt == max_retries - 1:
                logger.warning(f"Failed to geocode {location} after {max_retries} attempts")
                return None, None
            time.sleep(2 ** attempt)  # Exponential backoff

def geocode_with_fallback(row):
    if row['postcode']:
        lat, lon = geocode(row['postcode'])
        if lat and lon:
            return lat, lon
    return geocode(row['address'], is_postcode=False)

def main():
    # Read the CSV file
    logger.info(f"Reading CSV file: {args.input}")
    df = pd.read_csv(args.input)
    logger.info(f"Loaded {len(df)} rows from CSV")

    # Extract postcodes
    logger.info("Extracting postcodes from addresses")
    df['postcode'] = df['address'].apply(extract_postcode)
    logger.info(f"Extracted {df['postcode'].notna().sum()} postcodes")

    # Apply geocoding with fallback and rate limiting
    logger.info("Starting geocoding process")
    total_addresses = len(df)

    results = []
    for index, row in tqdm(df.iterrows(), total=total_addresses, desc="Geocoding", unit="address"):
        lat, lon = geocode_with_fallback(row)
        results.append((lat, lon))
        time.sleep(args.delay)  # Add a delay after each geocoding request

    df['lat'], df['lon'] = zip(*results)

    # Save the cache after processing
    save_cache()

    # Count successful geocodes
    successful_geocodes = df['lat'].notna().sum()

    logger.info(f"Finished geocoding process. Successfully geocoded {successful_geocodes} out of {total_addresses} addresses.")

    # Save the updated DataFrame to a new CSV file
    logger.info(f"Saving results to CSV: {args.output}")
    df.to_csv(args.output, index=False)
    logger.info(f"Results saved to {args.output}")
    logger.info(f"Total addresses processed: {total_addresses}")
    logger.info(f"Successfully geocoded: {successful_geocodes}")
    logger.info(f"Success rate: {successful_geocodes/total_addresses:.2%}")

if __name__ == "__main__":
    main()
