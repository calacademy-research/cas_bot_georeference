import argparse

import pandas as pd

from request_geolocate import Geolocate
from process_gvs import GVSProcess
from clean_coords import CleanCoords
import logging

class ProcessAll:
    def __init__(self, cli_args: dict):
        # Setup logger
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.DEBUG if cli_args.get("verbose") else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger.info("Running GEOLocate...")
        self.geolocate = Geolocate(cli_args)
        self.geo_csv = self.geolocate.geocoded_data

        self.logger.info("Initializing and running GVS...")
        self.gvs_process = GVSProcess(geocoded_csv=self.geo_csv)

        self.gvs_checked = self.gvs_process.process_csv_gvs()

        self.logger.info("Initializing and cleaning coordinates...")
        self.clean_coords = CleanCoords(self.gvs_checked)
        self.logger.info("Pipeline completed.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the full geolocation pipeline.')
    parser.add_argument('--cache-db', type=str, default=None, help='SQLite cache DB filename')
    parser.add_argument('-t', '--delay', type=float, default=0.6, help='Delay between GEOLocate API calls')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')
    parser.add_argument('--country', default='country', help='Country field name')
    parser.add_argument('--state', default='state', help='State field name')
    parser.add_argument('--county', default='county', help='County field name')
    parser.add_argument('--locality', default='locality', help='Locality field name')

    args = parser.parse_args()
    arg_dict = vars(args)  # Convert Namespace to dict

    process_all = ProcessAll(arg_dict)
