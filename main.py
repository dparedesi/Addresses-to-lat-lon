import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import logging
import sys
from tqdm import tqdm
import pickle
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read('geocoding_config.ini')

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.get('Logging', 'level', fallback='INFO').upper()),
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

def load_cache():
    """Load the geocoding cache from a file."""
    try:
        with open(CACHE_FILE, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return {}

def save_cache():
    """Save the geocoding cache to a file."""
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(geocode_cache, f)

# Load the cache at the start
geocode_cache = load_cache()

def extract_postcode(address):
    """Extract a UK postcode from an address string."""
    pattern = r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b'
    match = re.search(pattern, address)
    return match.group(0) if match else None

def geocode(location, is_postcode=True):
    """
    Geocode a location using Nominatim.
    
    Args:
    location (str): The location to geocode (postcode or full address).
    is_postcode (bool): Whether the location is a postcode.

    Returns:
    tuple: (latitude, longitude) or (None, None) if geocoding fails.
    """
    if location in geocode_cache:
        return geocode_cache[location]

    geolocator = Nominatim(user_agent=USER_AGENT)
    for attempt in range(MAX_RETRIES):
        try:
            query = f"{location}, UK" if is_postcode else location
            location_result = geolocator.geocode(query)
            if location_result:
                result = (location_result.latitude, location_result.longitude)
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
    """Attempt to geocode using postcode first, then fall back to full address."""
    if row['postcode']:
        lat, lon = geocode(row['postcode'])
        if lat and lon:
            return lat, lon
    return geocode(row['address'], is_postcode=False)

def main():
    logger.info(f"Reading CSV file: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows from CSV")

    logger.info("Extracting postcodes from addresses")
    df['postcode'] = df['address'].apply(extract_postcode)
    logger.info(f"Extracted {df['postcode'].notna().sum()} postcodes")

    logger.info("Starting geocoding process")
    total_addresses = len(df)

    results = []
    for index, row in tqdm(df.iterrows(), total=total_addresses, desc="Geocoding", unit="address"):
        lat, lon = geocode_with_fallback(row)
        results.append((lat, lon))
        logger.debug(f"Geocoded: {row['address']} -> ({lat}, {lon})")
        time.sleep(RATE_LIMIT)  # Add a delay after each geocoding request

    df['lat'], df['lon'] = zip(*results)

    save_cache()
    logger.debug("Cache saved")

    successful_geocodes = df['lat'].notna().sum()
    logger.info(f"Finished geocoding process. Successfully geocoded {successful_geocodes} out of {total_addresses} addresses.")

    logger.info(f"Saving results to CSV: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Results saved to {OUTPUT_FILE}")
    logger.info(f"Total addresses processed: {total_addresses}")
    logger.info(f"Successfully geocoded: {successful_geocodes}")
    logger.info(f"Success rate: {successful_geocodes/total_addresses:.2%}")

if __name__ == "__main__":
    main()
