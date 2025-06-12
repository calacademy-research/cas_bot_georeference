#!/usr/bin/env python3
"""
Geolocate CSV(s) via GEOLocate webservice (Python 3.12)

Processes all .csv files in geo_csvs/input_csv/, concatenates them,
georeferences using GEOLocate API, and stores results in self.geocoded_data.
"""

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
import bels_reformat
import pandas as pd
import requests
import requests_cache


class Geolocate:
    """Processes all CSVs in geo_csvs/input_csv and returns georeferenced results as a DataFrame."""

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
        self.args = self._dict_to_namespace(params or {})
        self.geocoded_data = pd.DataFrame()

        logging.basicConfig(
            level=logging.DEBUG if self.args.verbose else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        requests_cache.install_cache(
            cache_name=self.args.cache_db or 'geolocate_cache',
            backend='sqlite',
            expire_after=None
        )

        self._process()

    @staticmethod
    def _dict_to_namespace(d: dict):
        """Convert dict to SimpleNamespace with defaults."""
        ns = SimpleNamespace(
            delay=d.get('delay', 0.6),
            verbose=d.get('verbose', False),
            cache_db=d.get('cache_db', None),
            country=d.get('country', 'country'),
            state=d.get('state', 'state'),
            county=d.get('county', 'county'),
            locality=d.get('locality', 'locality')
        )
        return ns

    def _georef(self, user_params: dict) -> list['Geolocate.Result']:
        """Calls GEOLocate API with user params, returns parsed result list."""
        params = {**self.DEFAULT_OPTS, **user_params, 'fmt': 'json'}
        logging.debug(f"Requesting GEOLocate API with params: {params}")
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

    def _decimal_places(self, val):
        """Returns the number of decimal places in a float or stringified float."""
        try:
            s = str(val)
            if '.' in s:
                return len(s.split('.')[-1].rstrip('0'))
        except Exception:
            pass
        return 0


    def _round_coords(self):
        """Rounds Geo_Lat and Geo_Lon to the least precise decimal place between the two per row."""

        def _round_row(row):
            lat = row['Geo_Lat']
            lon = row['Geo_Lon']
            try:
                lat_dp = self._decimal_places(lat)
                lon_dp = self._decimal_places(lon)
                target_dp = min(lat_dp, lon_dp)

                if pd.notnull(lat) and pd.notnull(lon):
                    row['Geo_Lat'] = round(float(lat), target_dp)
                    row['Geo_Lon'] = round(float(lon), target_dp)
            except Exception:
                pass
            return row

        self.geocoded_data = self.geocoded_data.apply(_round_row, axis=1)


    def _load_and_concat_csvs(self, folder: Path) -> pd.DataFrame:
        """Loads and concatenates all CSV files from the input folder."""
        all_csvs = sorted(folder.glob("*.csv"))
        if not all_csvs:
            raise FileNotFoundError(f"No CSV files found in {folder}")
        logging.info(f"Loading {len(all_csvs)} files from {folder}")
        return pd.concat([pd.read_csv(f) for f in all_csvs], ignore_index=True)

    def _process(self):
        input_folder = Path("geo_csvs/input_csv")
        df = self._load_and_concat_csvs(input_folder)
        df = bels_reformat.rename_drop_columns(df)

        df.reset_index(inplace=True)  # Keep track of original row order
        all_results = []

        for idx, row in df.iterrows():



            row_data = {
                'index': idx,
                'country': row.get('country', ''),
                'stateprovince': row.get('stateprovince', ''),
                'county': row.get('county', ''),
                'locality': row.get('locality', ''),
                'bels_match': row.get('bels_match', False),
                'Geo_Source': '',
                'Geo_NumResults': '',
                'Geo_ResultID': '',
                'Geo_Lat': '',
                'Geo_Lon': '',
                'Geo_UncertaintyM': '',
                'Geo_Score': '',
                'Geo_Precision': '',
                'Geo_ParsePattern': '',
                'datum': row.get('datum', ''),
                'coordinate_uncertainty_meters': row.get('coordinate_uncertainty_meters', ''),
            }

            if row_data['bels_match']:
                # Use BELS output
                row_data['Geo_Lat'] = row.get('latitude')
                row_data['Geo_Lon'] = row.get('longitude')
                row_data['Geo_UncertaintyM'] = row.get('coordinate_uncertainty_meters')
                row_data['Geo_ResultID'] = ''
                row_data['Geo_Source'] = 'bels'
            else:
                # Use GEOLocate
                params = {
                    'country': row_data['country'],
                    'state': row_data['stateprovince'],
                    'county': row_data['county'],
                    'locality': row_data['locality']
                }
                logging.debug(f"Sending GEOLocate query: {params}")
                results = self._georef(params)
                logging.debug(f"Recieving GEOLocate results: {results}")
                row_data['Geo_Source'] = 'geolocate'
                row_data['Geo_NumResults'] = len(results)

                if results:
                    res = results[0]
                    row_data.update({
                        'Geo_ResultID': 1,
                        'Geo_Lat': res.latitude,
                        'Geo_Lon': res.longitude,
                        'Geo_UncertaintyM': res.uncertainty_radius_m,
                        'Geo_Score': res.score,
                        'Geo_Precision': res.precision,
                        'Geo_ParsePattern': res.parse_pattern
                    })

                time.sleep(self.args.delay)

            all_results.append(row_data)

        self.geocoded_data = pd.DataFrame(all_results)

        columns_to_drop = ['latitude', 'longitude', 'latitude_x', 'longitude_x',
                           'country_x', 'country_y', 'state', 'stateprovince_x', 'stateprovince_y',
                           'county_x', 'county_y']

        self.geocoded_data.drop(columns=[col for col in columns_to_drop if col in self.geocoded_data.columns],
                                inplace=True)

        self._round_coords()
