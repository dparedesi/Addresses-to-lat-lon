import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import logging
from tqdm import tqdm
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add a simple cache
geocode_cache = {}

def load_cache():
    try:
        with open('geocode_cache.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_cache():
    with open('geocode_cache.json', 'w') as f:
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
        return None, None

def geocode_with_fallback(row):
    if row['postcode']:
        lat, lon = geocode(row['postcode'])
        if lat and lon:
            return lat, lon
    return geocode(row['address'], is_postcode=False)

# Read the CSV file
logger.info("Reading CSV file")
df = pd.read_csv('inputs-outputs/input_locations.csv')
logger.info(f"Loaded {len(df)} rows from CSV")

# Extract postcodes
logger.info("Extracting postcodes from addresses")
df['postcode'] = df['address'].apply(extract_postcode)
logger.info(f"Extracted {df['postcode'].notna().sum()} postcodes")

# Apply geocoding with fallback and rate limiting
logger.info("Starting geocoding process")
total_addresses = len(df)
successful_geocodes = 0

results = []
for index, row in tqdm(df.iterrows(), total=total_addresses, desc="Geocoding", unit="address"):
    lat, lon = geocode_with_fallback(row)
    results.append((lat, lon))
    time.sleep(1)  # Add a 1-second delay after each geocoding request

df['lat'], df['lon'] = zip(*results)

# Save the cache after processing
save_cache()

# Count successful geocodes
successful_geocodes = df['lat'].notna().sum()

logger.info(f"Finished geocoding process. Successfully geocoded {successful_geocodes} out of {total_addresses} addresses.")

# Save the updated DataFrame to a new CSV file
logger.info("Saving results to CSV")
df.to_csv('inputs-outputs/output_lat_lon.csv', index=False)
logger.info("Results saved to output_lat_lon.csv")
logger.info(f"Total addresses processed: {total_addresses}")
logger.info(f"Successfully geocoded: {successful_geocodes}")
logger.info(f"Success rate: {successful_geocodes/total_addresses:.2%}")
