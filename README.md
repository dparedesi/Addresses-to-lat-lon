# Geocoding Address Data

This repository contains two Python scripts for geocoding address data: a basic model and an advanced model. Both scripts take a CSV file containing addresses and convert them into geographic coordinates (latitude and longitude).

## Files

1. `00.basic_model.py`: A simple implementation of the geocoding process.
2. `01.advanced_model.py`: An enhanced version with additional features and optimizations.
3. `geocoding_config.ini`: Configuration file for the advanced model.

## Basic Model (`00.basic_model.py`)

This script provides a straightforward approach to geocoding addresses:

- Reads addresses from a CSV file
- Extracts postcodes from the addresses
- Uses the Nominatim geocoder to convert addresses to coordinates
- Implements a simple fallback mechanism (tries postcode first, then full address)
- Saves the results to a new CSV file

Features:
- Basic error handling
- Progress bar using tqdm
- Logging of the geocoding process

## Advanced Model (`01.advanced_model.py`)

This script builds upon the basic model and adds several improvements:

- Configurable settings using an INI file
- Caching of geocoding results to reduce API calls
- Rate limiting to respect API usage policies
- Exponential backoff for failed requests
- Multi-processing for faster execution
- Enhanced error handling and input validation

Additional Features:
- Configurable input/output file names
- Adjustable rate limiting and retry attempts
- Cached results stored using joblib

## Configuration (`geocoding_config.ini`)

This file contains settings for the advanced model, including:

- API user agent
- Rate limiting parameters
- Input/output file names
- Cache file location

## Requirements

- Python 3.6+
- pandas
- geopy
- tqdm
- ratelimit (for advanced model)
- joblib (for advanced model)
- configparser (for advanced model)

## Usage

1. Install the required packages:
   ```
   pip install pandas geopy tqdm ratelimit joblib configparser
   ```

2. Prepare your input CSV file with an 'address' column.

3. For the basic model:
   ```
   python 00.basic_model.py
   ```

4. For the advanced model:
   - Adjust settings in `geocoding_config.ini` if needed
   - Run:
     ```
     python 01.advanced_model.py
     ```

5. Check the output CSV file for results.

## Note

These scripts use the Nominatim geocoder, which has usage limitations. Please review and comply with Nominatim's usage policy when using these scripts.