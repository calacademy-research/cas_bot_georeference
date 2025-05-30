"""This file will be for post-processing cleaning of Lat/Long coordinates.
Occupying the last major step of the pipeline. Geolocate --> GVS --> Cleaning"""
import csv
import sqlite3
import pandas as pd
import re
from rapidfuzz import fuzz

class CleanCoords:
    def __init__(self, processed_csv):
        self.final_csv = processed_csv
        self.conn = None
        self.gazetteer_df = None
        self.create_new_sqlite_gazetteer()
        self.initial_filter_results()
        self.apply_fuzzy_matching()
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

    def extract_place_name(self, locality):
        tokens = self.normalize_locality_string(locality)
        for token in tokens:
            match = self.fuzzy_match_place(token, self.gazetteer_df)
            if match is not None:
                return {
                    'matched_name': match['place_name'],
                    'matched_lat': match['Latitude'],
                    'matched_lon': match['Longitude']
                }
        return {'matched_name': None, 'matched_lat': None, 'matched_lon': None}

    def normalize_locality_string(self, text):
        if pd.isna(text):
            return []
        text = text.lower()
        text = re.sub(r'[^\w\s,]', '', text)
        return [t.strip() for t in text.split(',')]

    def fuzzy_match_place(self, token, gazetteer):
        scores = gazetteer['place_name'].apply(lambda name: fuzz.partial_ratio(token, name.lower()))
        best_idx = scores.idxmax()
        if scores[best_idx] > 80:
            return gazetteer.loc[best_idx]
        return None

    def apply_fuzzy_matching(self):
        match_results = self.final_csv['locality'].apply(self.extract_place_name)
        match_df = pd.DataFrame(match_results.tolist())
        self.final_csv = pd.concat([self.final_csv.reset_index(drop=True), match_df.reset_index(drop=True)], axis=1)

    def placeholder_function(self):
        self.final_csv.to_csv("geo_csvs/output_csv/final_output.csv", index=False, encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)
        print("Coordinates Cleaned!")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("SQLite connection closed.")
