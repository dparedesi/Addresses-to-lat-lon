# Geocoding Address Data to lat/lon

This project provides a Python script for geocoding address data. It takes a CSV file containing addresses and converts them into geographic coordinates (latitude and longitude).

## Features

- Reads addresses from a CSV file
- Extracts postcodes from addresses
- Uses the Nominatim geocoder to convert addresses to coordinates
- Implements a fallback mechanism (tries postcode first, then full address)
- Caches geocoding results to reduce API calls
- Configurable settings using an INI file
- Rate limiting to respect API usage policies
- Exponential backoff for failed requests
- Logging of the geocoding process

## Requirements

- Python 3.6+
- pandas
- geopy
- tqdm
- configparser

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/dparedesi/addresses-to-lat-lon.git
   cd addresses-to-lat-lon
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Configuration

Edit the `geocoding_config.ini` file to customize the settings:

- `user_agent`: Identify your application to the geocoding service
- `rate_limit`: Delay in seconds between geocoding requests
- `max_retries`: Maximum number of retry attempts for failed requests
- `input_csv`: Path to the input CSV file
- `output_csv`: Path to the output CSV file
- `cache_file`: Path to the cache file
- `logging.level`: Set the logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL)

## Usage

1. Prepare your input CSV file with an 'address' column.

2. Run the script:
   ```
   python main.py
   ```

3. Check the output CSV file for results.

## Output

The script will generate:
- An output CSV file with the original addresses and their corresponding latitude and longitude
- A log file (`geocoding.log`) with detailed information about the geocoding process
- A cache file to store previously geocoded addresses

## Note

This script uses the Nominatim geocoder, which has usage limitations. Please review and comply with Nominatim's usage policy when using this script.

## License

[MIT License](LICENSE)