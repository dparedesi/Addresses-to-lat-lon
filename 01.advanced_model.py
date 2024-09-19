import pandas as pd
import numpy as np
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
from ratelimit import limits, sleep_and_retry
import logging
from tqdm import tqdm
from backoff import on_exception, expo
import requests
from multiprocessing import Pool, cpu_count
import joblib
import configparser

config = configparser.ConfigParser()
config.read('geocoding_config.ini')

# Use config values
input_file = config['Files']['input_csv']
output_file = config['Files']['output_csv']
cache_file = config['Files']['cache_file']
user_agent = config['Geocoding']['user_agent']
RATE_LIMIT = int(config['Geocoding']['rate_limit'])
MAX_RETRIES = int(config['Geocoding']['max_retries'])

# Set up logging
logging.basicConfig(filename='geocoding.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CALLS = 1

try:
    geocode_cache = joblib.load(cache_file)
except FileNotFoundError:
    geocode_cache = {}

@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT)
@on_exception(expo, (GeocoderTimedOut, GeocoderServiceError, requests.exceptions.RequestException), max_tries=MAX_RETRIES)
def geocode(location, is_postcode=True):
    geolocator = Nominatim(user_agent=user_agent)
    try:
        query = f"{location}, UK" if is_postcode else location
        location = geolocator.geocode(query)
        if location:
            return location.latitude, location.longitude
        return None, None
    except (GeocoderTimedOut, GeocoderServiceError, requests.exceptions.RequestException):
        return None, None

def geocode_with_cache(location, is_postcode=True):
    if location in geocode_cache:
        return geocode_cache[location]
    result = geocode(location, is_postcode)
    geocode_cache[location] = result
    joblib.dump(geocode_cache, cache_file)
    return result

def geocode_with_fallback(row):
    if row['postcode']:
        lat, lon = geocode_with_cache(row['postcode'])
        if lat and lon:
            return lat, lon
    return geocode_with_cache(row['address'], is_postcode=False)

def validate_address(address):
    if not isinstance(address, str) or len(address.strip()) == 0:
        raise ValueError("Invalid address")
    return address.strip()

def extract_postcode(address):
    pattern = r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b'
    match = re.search(pattern, address)
    return match.group(0) if match else None

def process_chunk(chunk):
    return chunk.apply(geocode_with_fallback, axis=1)

def main():
    # Read the CSV file
    logger.info("Reading CSV file")
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} rows from CSV")

    # Apply validation to addresses
    df['address'] = df['address'].apply(validate_address)

    # Extract postcodes
    logger.info("Extracting postcodes from addresses")
    df['postcode'] = df['address'].apply(extract_postcode)
    logger.info(f"Extracted {df['postcode'].notna().sum()} postcodes")

    # Apply geocoding with fallback
    logger.info("Starting geocoding process")
    total_addresses = len(df)

    # Use multiprocessing for faster processing
    num_processes = cpu_count()
    df_split = np.array_split(df, num_processes)
    pool = Pool(num_processes)

    # Use tqdm for progress bar
    with tqdm(total=total_addresses, desc="Geocoding", unit="address") as pbar:
        results = []
        for result in pool.imap(process_chunk, df_split):
            results.append(result)
            pbar.update(len(result))

    df = pd.concat(results)

    # Count successful geocodes
    successful_geocodes = df['lat'].notna().sum()

    logger.info(f"Finished geocoding process. Successfully geocoded {successful_geocodes} out of {total_addresses} addresses.")

    # Save the updated DataFrame to the output CSV file
    logger.info(f"Saving results to {output_file}")
    df.to_csv(output_file, index=False)
    logger.info(f"Results saved to {output_file}")

    # Print summary
    print(f"\nSummary:")
    print(f"Total addresses processed: {total_addresses}")
    print(f"Successfully geocoded: {successful_geocodes}")
    print(f"Success rate: {successful_geocodes/total_addresses:.2%}")

if __name__ == '__main__':
    main()
