#!/usr/bin/env python3
"""
Geolocate CSV(s) via GEOLocate webservice (Python 3.12)

Supports both CLI and programmatic usage.
Always returns only the best (first) result per record, and stores results as a DataFrame in `self.geocoded_data`.
"""

import argparse
import csv
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import requests
import requests_cache


class Geolocate:
    """Encapsulates GEOLocate CSV processing logic, returning results as a pandas DataFrame."""

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
        self.args = self._dict_to_namespace(params) if params else self._parse_args()
        self.geocoded_data = pd.DataFrame()

        if self.args.verbose:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

        # Setup persistent caching
        requests_cache.install_cache(
            cache_name=self.args.cache_db or 'geolocate_cache',
            backend='sqlite',
            expire_after=None
        )

        self._process()

    @staticmethod
    def _parse_args():
        parser = argparse.ArgumentParser(description='Geolocate CSV(s) via GEOLocate webservice')
        parser.add_argument('-i', '--input', type=Path, required=True, help='Input CSV file or folder')
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
        """Convert a plain dict to a SimpleNamespace object (argparse-like)."""
        ns = SimpleNamespace(**d)
        # Convert paths
        if hasattr(ns, 'input'):
            ns.input = Path(ns.input)
        return ns

    def _georef(self, user_params: dict) -> list['Geolocate.Result']:
        """Call the GEOLocate webservice with query params and return result objects."""
        params = {**self.DEFAULT_OPTS, **user_params, 'fmt': 'json'}
        logging.debug(f"Geolocate request params: {params}")
        resp = requests.get(self.ENDPOINT, params=params)
        resp.raise_for_status()

        results = []
        for feat in resp.json().get('resultSet', {}).get('features', []):
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

    def _process(self):
        """Processes the input CSV(s) and stores the geocoded result DataFrame in self.geocoded_data."""
        all_results = []

        if self.args.input.is_dir():
            input_files = sorted(self.args.input.glob('*.csv'))
        else:
            input_files = [self.args.input]

        for in_file in input_files:
            logging.info(f"Processing file: {in_file}")
            with in_file.open(newline='', encoding='utf8') as inf:
                reader = csv.DictReader(inf)
                if not reader.fieldnames:
                    logging.warning(f"Skipping {in_file}: no headers found.")
                    continue

                for idx, row in enumerate(reader, start=1):
                    logging.debug(f"Row {idx}: {row}")
                    params = {
                        'country': row.get(self.args.country, ''),
                        'state': row.get(self.args.state, ''),
                        'county': row.get(self.args.county, ''),
                        'locality': row.get(self.args.locality, '')
                    }
                    results = self._georef(params)
                    row['Geo_NumResults'] = len(results)

                    if results:
                        res = results[0]
                        row.update({
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
                        row.update({
                            'LocalityID': row.get("LocalityID", ''),
                            'Geo_ResultID': '',
                            'Geo_Lat': '',
                            'Geo_Lon': '',
                            'Geo_UncertaintyM': '',
                            'Geo_Score': '',
                            'Geo_Precision': '',
                            'Geo_ParsePattern': ''
                        })

                    all_results.append(row)
                    time.sleep(self.args.delay)

        self.geocoded_data = pd.DataFrame(all_results)

if __name__ == '__main__':
    Geolocate().run()