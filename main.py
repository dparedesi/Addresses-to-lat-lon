import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import logging
import sys
from tqdm import tqdm
import configparser
import pickle

# Load configuration
config = configparser.ConfigParser()
config.read('geocoding_config.ini')

# Get logging level from config
log_level_str = config.get('Logging', 'level', fallback='INFO')
log_level = getattr(logging, log_level_str.upper(), logging.INFO)

# Set up logging to file and console
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geocoding.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Get configuration values
INPUT_FILE = config['Files']['input_csv']
OUTPUT_FILE = config['Files']['output_csv']
CACHE_FILE = config['Files']['cache_file']
USER_AGENT = config['Geocoding']['user_agent']
RATE_LIMIT = int(config['Geocoding']['rate_limit'])
MAX_RETRIES = int(config['Geocoding']['max_retries'])

# Add a simple cache
geocode_cache = {}

def load_cache():
    try:
        with open(CACHE_FILE, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Error loading cache: {str(e)}")
        return {}

def save_cache():
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(geocode_cache, f)

# Load the cache at the start
geocode_cache = load_cache()

def extract_postcode(address):
    pattern = r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b'
    match = re.search(pattern, address)
    return match.group(0) if match else None

def geocode(location, is_postcode=True):
    if location in geocode_cache:
        return geocode_cache[location]

    geolocator = Nominatim(user_agent=USER_AGENT)
    for attempt in range(MAX_RETRIES):
        try:
            if is_postcode:
                query = f"{location}, UK"
            else:
                query = location
            location_result = geolocator.geocode(query)
            if location_result:
                result = (location_result.latitude, location_result.longitude)
                # Only cache non-null results
                geocode_cache[location] = result
                return result
            else:
                return (None, None)
        except (GeocoderTimedOut, GeocoderServiceError):
            if attempt == MAX_RETRIES - 1:
                logger.warning(f"Failed to geocode {location} after {MAX_RETRIES} attempts")
                return None, None
            time.sleep(2 ** attempt)  # Exponential backoff

def geocode_with_fallback(row):
    if row['postcode']:
        lat, lon = geocode(row['postcode'])
        if lat and lon:
            return lat, lon
    return geocode(row['address'], is_postcode=False)

def clean_cache():
    global geocode_cache
    geocode_cache = {k: v for k, v in geocode_cache.items() if v[0] is not None and v[1] is not None}
    save_cache()

def main():
    logger.debug("Starting main function")
    # Read the CSV file
    logger.info(f"Reading CSV file: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows from CSV")

    # Extract postcodes
    logger.info("Extracting postcodes from addresses")
    df['postcode'] = df['address'].apply(extract_postcode)
    logger.debug(f"Postcode extraction complete")
    logger.info(f"Extracted {df['postcode'].notna().sum()} postcodes")

    # Apply geocoding with fallback and rate limiting
    logger.info("Starting geocoding process")
    total_addresses = len(df)

    results = []
    for index, row in tqdm(df.iterrows(), total=total_addresses, desc="Geocoding", unit="address"):
        lat, lon = geocode_with_fallback(row)
        results.append((lat, lon))
        logger.debug(f"Geocoded: {row['address']} -> ({lat}, {lon})")
        time.sleep(RATE_LIMIT)  # Add a delay after each geocoding request

    df['lat'], df['lon'] = zip(*results)

    # Save the cache after processing
    save_cache()
    logger.debug("Cache saved")

    # Count successful geocodes
    successful_geocodes = df['lat'].notna().sum()

    logger.info(f"Finished geocoding process. Successfully geocoded {successful_geocodes} out of {total_addresses} addresses.")

    # Save the updated DataFrame to a new CSV file
    logger.info(f"Saving results to CSV: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Results saved to {OUTPUT_FILE}")
    logger.info(f"Total addresses processed: {total_addresses}")
    logger.info(f"Successfully geocoded: {successful_geocodes}")
    logger.info(f"Success rate: {successful_geocodes/total_addresses:.2%}")

    logger.debug("Main function completed")

if __name__ == "__main__":
    main()
