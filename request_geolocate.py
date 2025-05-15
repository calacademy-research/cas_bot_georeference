#!/usr/bin/env python3
"""
Geolocate CSV(s) via GEOLocate webservice (Python 3.12)
Usage:
  # Single file:
  python geolocate.py -i in.csv -o out.csv [--cache-db cache.sqlite] [--delay 0.6]

  # Directory:
  python geolocate.py -i input_folder -o output_folder [--cache-db cache.sqlite] [--delay 0.6]

Encapsulated in one Geolocate class. Supports file or directory input/output.
Always returns only the best (first) result per record, and writes blank fields if no match.
This is a modified version of a demo
created by the Yale Peabody Museum Division of Informatics:https://github.com/YPM-Informatics/glc_py
"""
import argparse
import csv
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import requests
import requests_cache

class Geolocate:
    """Encapsulates GEOLocate CSV processing logic for files or directories.
       Uses SQLlite caching to speed up batch processing. Defines result headers and input options
    """
    @dataclass
    class Result:
        latitude: float
        longitude: float
        uncertainty_radius_m: float
        uncertainty_polygon: dict
        precision: str
        score: float
        parse_pattern: str
        displaced_distance_mi: float
        displaced_heading_deg: float
        debug: dict

    ENDPOINT = 'https://www.geo-locate.org/webservices/geolocatesvcv2/glcwrap.aspx'
    DEFAULT_OPTS = {
        'hwyX': 'true',
        'enableH2O': 'true',
        'doUncert': 'true',
        'doPoly': 'false',
        'displacePoly': 'false',
        'languageKey': '0'
    }

    def __init__(self, params: dict = None):
        if params:
            # Initialize from parameters
            self.args = self._dict_to_namespace(params)
        else:
            # Fallback to argparse CLI mode
            self.args = self._parse_args()

        if self.args.verbose:
            logging.basicConfig(level=logging.DEBUG)

        # Setup persistent caching (SQLite)
        requests_cache.install_cache(
            cache_name=self.args.cache_db or 'geolocate_cache',
            backend='sqlite',
            expire_after=None
        )

    @staticmethod
    def _parse_args():
        parser = argparse.ArgumentParser(description='Geolocate CSV(s) via GEOLocate webservice')
        parser.add_argument('-i', '--input', type=Path, required=True, help='Input CSV or folder')
        parser.add_argument('-o', '--output', type=Path, required=True, help='Output CSV or folder')
        parser.add_argument('--cache-db', type=str, default=None, help='SQLite cache DB filename')
        parser.add_argument('-t', '--delay', type=float, default=0.6, help='Delay between API calls')
        parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')
        parser.add_argument('--country', default='country', help='Country field name')
        parser.add_argument('--state', default='state', help='State field name')
        parser.add_argument('--county', default='county', help='County field name')
        parser.add_argument('--locality', default='locality', help='Locality field name')
        return parser.parse_args()

    @staticmethod
    def _dict_to_namespace(d: dict):
        """Convert a plain dict to an argparse.Namespace object."""
        from types import SimpleNamespace
        ns = SimpleNamespace(**d)
        # Ensure Path conversion
        for key in ['input', 'output']:
            if hasattr(ns, key):
                setattr(ns, key, Path(getattr(ns, key)))
        return ns

    def _georef(self, user_params: dict) -> list['Geolocate.Result']:
        """requests the geolocate endpoint with a user params dict on a row by row basis
            args:
                user_params: a dictionary of default inputs, concats with the default options
            returns:
                result: a list of dictionaries, used to create a dataframe in postprocessing
        """
        params = {**self.DEFAULT_OPTS, **user_params, 'fmt': 'json'}
        logging.debug(f"Geolocate request params: {params}")
        resp = requests.get(self.ENDPOINT, params=params)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for feat in data.get('resultSet', {}).get('features', []):
            p = feat['properties']
            lon, lat = feat['geometry']['coordinates']
            results.append(self.Result(
                latitude=lat,
                longitude=lon,
                uncertainty_radius_m=p.get('uncertaintyRadiusMeters'),
                uncertainty_polygon=p.get('uncertaintyPolygon'),
                precision=p.get('precision'),
                score=p.get('score'),
                parse_pattern=p.get('parsePattern'),
                displaced_distance_mi=p.get('displacedDistanceMiles'),
                displaced_heading_deg=p.get('displacedHeadingDegrees'),
                debug=p.get('debug', {})
            ))
        return results

    def _process_file(self, in_path: Path, out_path: Path):
        """Process a single CSV and batch processes it with the georeference endpoint.
           args:
                in_path: the path of the input csv
                out_path: the paths to write the output csv to
           returns:
                None
           """
        logging.info(f"Processing file: {in_path} -> {out_path}")
        with in_path.open(newline='', encoding='utf8') as inf, \
             out_path.open('w', newline='', encoding='utf8') as outf:
            reader = csv.DictReader(inf)
            if not reader.fieldnames:
                logging.warning(f"Skipping {in_path}: no headers found.")
                return
            fieldnames = reader.fieldnames + [
                'LocalityID', 'Geo_ResultID', 'Geo_Lat', 'Geo_Lon',
                'Geo_UncertaintyM', 'Geo_Score', 'Geo_Precision',
                'Geo_ParsePattern', 'Geo_NumResults'
            ]
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()

            for idx, row in enumerate(reader, start=1):
                logging.debug(f"Row {idx}: {row}")
                params = {
                    'country': row.get(self.args.country, ''),
                    'state': row.get(self.args.state, ''),
                    'county': row.get(self.args.county, ''),
                    'locality': row.get(self.args.locality, '')
                }
                results = self._georef(params)
                # always output one row per input, with best result or blanks
                row['Geo_NumResults'] = len(results)
                if results:
                    res = results[0]
                    out = row.copy()
                    out.update({
                        'LocalityID': row.get("LocalityID", ''),
                        'Geo_ResultID': 1,
                        'Geo_Lat': res.latitude,
                        'Geo_Lon': res.longitude,
                        'Geo_UncertaintyM': res.uncertainty_radius_m,
                        'Geo_Score': res.score,
                        'Geo_Precision': res.precision,
                        'Geo_ParsePattern': res.parse_pattern
                    })
                else:
                    out = row.copy()
                    out.update({
                        'LocalityID': row.get("LocalityID", ''),
                        'Geo_ResultID': '',
                        'Geo_Lat': '',
                        'Geo_Lon': '',
                        'Geo_UncertaintyM': '',
                        'Geo_Score': '',
                        'Geo_Precision': '',
                        'Geo_ParsePattern': ''
                    })
                writer.writerow(out)
                time.sleep(self.args.delay)

    def run(self):
        """Runs main script with command line args , running the main process_file function"""
        args = self.args
        # Determine input files
        if args.input.is_dir():
            input_files = sorted(args.input.glob('*.csv'))
        else:
            input_files = [args.input]
        # Prepare outputs
        if args.output.exists() and args.output.is_dir():
            out_dir = args.output
        else:
            if len(input_files) > 1:
                args.output.mkdir(parents=True, exist_ok=True)
                out_dir = args.output
            else:
                out_dir = None
        # Process each file
        for in_file in input_files:
            out_file = out_dir / in_file.name if out_dir else args.output
            self._process_file(in_file, out_file)

if __name__ == '__main__':
    Geolocate().run()