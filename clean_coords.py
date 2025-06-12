import csv
import sqlite3
import pandas as pd
import subprocess
import tempfile
import os

class CleanCoords:
    def __init__(self, processed_csv, logger):
        self.final_csv = processed_csv
        self.logger = logger
        self.conn = None
        self.gazetteer_df = None

        self.logger.info("Initializing and cleaning coordinates...")
        self.create_new_sqlite_gazetteer()
        self.initial_filter_results()
        self.clean_coordinates_with_r()
        self.placeholder_function()

    def create_new_sqlite_gazetteer(self):
        gazetteer_csv = pd.read_csv("geo_csvs/test_csvs/CA_gazetteer.csv")
        self.conn = sqlite3.connect("geolocate_cache.sqlite")
        table_name = "geo_gazetteer"

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?;
        """, (table_name,))
        table_exists = cursor.fetchone()

        if not table_exists:
            gazetteer_csv.to_sql(table_name, self.conn, if_exists='fail', index=False)
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists. Skipping creation.")

        self.gazetteer_df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
        self.gazetteer_df['place_name'] = self.gazetteer_df['place_name'].astype(str)

    def initial_filter_results(self):
        self.final_csv['com_georef'] = False
        self.final_csv['gvs_county'] = self.final_csv['gvs_county'] + " County"

        missing_coords = self.final_csv['latitude'].isna() | self.final_csv['longitude'].isna()
        latlong_err_invalid = self.final_csv['latlong_err'].isin(["in ocean", "Coordinate values out of bounds"])
        county_mismatch = self.final_csv['gvs_county'].str.strip().str.lower() != self.final_csv['county'].str.strip().str.lower()
        country_mismatch = self.final_csv['gvs_country'].str.strip().str.lower() != self.final_csv['country'].str.strip().str.lower()
        stateprovince_mismatch = self.final_csv['gvs_state'].str.strip().str.lower() != self.final_csv['stateprovince'].str.strip().str.lower()
        locality_blank = self.final_csv['locality'].isna() | (self.final_csv['locality'].str.strip() == '')
        centroid_missing_flag = locality_blank & (self.final_csv['latlong_err'] != "Possible centroid")

        self.final_csv['com_georef'] = (
            missing_coords |
            latlong_err_invalid |
            county_mismatch |
            country_mismatch |
            stateprovince_mismatch |
            centroid_missing_flag
        )

    def clean_coordinates_with_r(self):
        # Filter rows to check
        to_check = self.final_csv[self.final_csv['com_georef'] == False].copy()
        to_check = to_check[
            pd.to_numeric(to_check['latitude'], errors='coerce').between(-90, 90) &
            pd.to_numeric(to_check['longitude'], errors='coerce').between(-180, 180)
            ].copy()

        # Create temp input/output paths in the r_coord_clean directory
        coord_clean_dir = "r_coord_clean"
        os.makedirs(coord_clean_dir, exist_ok=True)

        input_path = os.path.join(coord_clean_dir, "temp_input.csv")
        output_path = os.path.join(coord_clean_dir, "temp_output.csv")

        # Write input CSV
        to_check.to_csv(input_path, index=False)


        try:
            subprocess.run([
                "Rscript", os.path.join(coord_clean_dir, "clean_coordinates.R"),
                input_path, output_path
            ], check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Rscript failed: {e}")
            raise

        # Read cleaned result
        cleaned = pd.read_csv(output_path)

        #converting ID to int64
        to_check['id'] = to_check['id'].astype('Int64')
        self.final_csv['id'] = self.final_csv['id'].astype('Int64')

        to_check = to_check.merge(cleaned[['id', 'cc_valid']], on='id', how='left')

        # Update flags in final DataFrame
        self.final_csv = self.final_csv.merge(to_check[['id', 'cc_valid']], on='id', how='left')
        self.final_csv['cc_valid'] = self.final_csv['cc_valid'].fillna(False)
        self.final_csv.loc[self.final_csv['cc_valid'] == False, 'com_georef'] = True

        # Cleanup
        os.remove(input_path)
        os.remove(output_path)

    def placeholder_function(self):
        self.final_csv.to_csv("geo_csvs/output_csv/all_output.csv", index=False,
                              encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        self.final_csv[self.final_csv['cc_valid']].to_csv("geo_csvs/output_csv/valid_output.csv", index=False,
                                                           encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        self.final_csv[~self.final_csv['cc_valid']].to_csv("geo_csvs/output_csv/flagged_output.csv", index=False,
                                                            encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        print("Coordinates cleaned and files written.")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("SQLite connection closed.")
