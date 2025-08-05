import argparse
import os.path

import pandas as pd
from pathlib import Path
from request_geolocate import Geolocate
from process_gvs import GVSProcess
from clean_coords import CleanCoords
from grouper import *
import logging

class ProcessAll:
    def __init__(self, cli_args: dict):
        # Setup logger
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.DEBUG if cli_args.get("verbose") else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger.info("Running Grouper")

        input_folder = Path("geo_csvs/input_csv")

        self.geo_csv = self._load_and_concat_csvs(folder=input_folder)

        df, key_df = grouper_main(self.geo_csv)

        # grouped_localities key
        key_df.to_csv(f"geo_csvs{os.path.sep}output_csv{os.path.sep}grouper_df_key.csv", sep=",", quotechar='"')


        self.logger.info("Running GEOLocate...")
        self.geolocate = Geolocate(df, cli_args)
        self.geo_csv = self.geolocate.geocoded_data
        #
        self.logger.info("Initializing and running GVS...")
        self.gvs_process = GVSProcess(geocoded_csv=self.geo_csv)
        #
        self.gvs_checked = self.gvs_process.process_csv_gvs()

        self.logger.info("Initializing and cleaning coordinates...")
        self.clean_coords = CleanCoords(self.gvs_checked, logger=self.logger)
        self.logger.info("Pipeline completed.")


    def _load_and_concat_csvs(self, folder: Path) -> pd.DataFrame:
        """Loads and concatenates all CSV files from the input folder."""
        all_csvs = sorted(folder.glob("*.csv"))
        if not all_csvs:
            raise FileNotFoundError(f"No CSV files found in {folder}")
        logging.info(f"Loading {len(all_csvs)} files from {folder}")
        return pd.concat([pd.read_csv(f, quotechar='"', sep=",") for f in all_csvs], ignore_index=True)


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
