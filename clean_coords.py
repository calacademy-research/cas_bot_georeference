import csv
import pandas as pd
import subprocess
import os
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

class CleanCoords:
    def __init__(self, processed_csv, logger):
        self.final_csv = processed_csv
        self.logger = logger
        self.conn = None
        self.logger.info("Initializing and cleaning coordinates...")
        self.initial_filter_results()
        self.clean_coordinates_with_r()
        self.detect_outliers_by_county()
        self.write_output_csvs()

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
        # review after meeting.
        duplicated_coords = (
                self.final_csv.duplicated(subset=['latitude', 'longitude'], keep=False)
                & (self.final_csv['latlong_err'] != 'Possible centroid')
        )


        self.final_csv['com_georef'] = (
            missing_coords |
            latlong_err_invalid |
            county_mismatch |
            country_mismatch |
            stateprovince_mismatch |
            centroid_missing_flag |
            duplicated_coords
        )

    def clean_coordinates_with_r(self):
        to_check = self.final_csv[self.final_csv['com_georef'] == False].copy()
        to_check = to_check[
            pd.to_numeric(to_check['latitude'], errors='coerce').between(-90, 90) &
            pd.to_numeric(to_check['longitude'], errors='coerce').between(-180, 180)
        ].copy()

        coord_clean_dir = "r_coord_clean"
        os.makedirs(coord_clean_dir, exist_ok=True)

        input_path = os.path.join(coord_clean_dir, "temp_input.csv")
        output_path = os.path.join(coord_clean_dir, "temp_output.csv")

        to_check.to_csv(input_path, index=False)

        try:
            subprocess.run([
                "Rscript", os.path.join(coord_clean_dir, "clean_coordinates.R"),
                input_path, output_path
            ], check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Rscript failed: {e}")
            raise

        cleaned = pd.read_csv(output_path)

        to_check['id'] = to_check['id'].astype('Int64')
        self.final_csv['id'] = self.final_csv['id'].astype('Int64')

        to_check = to_check.merge(cleaned[['id', 'cc_valid']], on='id', how='left')

        self.final_csv = self.final_csv.merge(to_check[['id', 'cc_valid']], on='id', how='left')
        self.final_csv['cc_valid'] = self.final_csv['cc_valid'].fillna(False)
        self.final_csv.loc[self.final_csv['cc_valid'] == False, 'com_georef'] = True

        os.remove(input_path)
        os.remove(output_path)

    def detect_outliers_by_county(self, contamination=0.1):
        self.final_csv['outlier_score'] = 1

        # Filter to only non-com_georef rows
        not_georef = self.final_csv[self.final_csv['com_georef'] == False]

        counties = not_georef['county'].dropna().unique()

        for county in counties:
            subset = not_georef[not_georef['county'] == county].copy()
            coords = subset[['latitude', 'longitude']].dropna().values

            if len(coords) < 10:
                continue

            coords_std = StandardScaler().fit_transform(coords)

            dbscan = DBSCAN(eps=0.5, min_samples=5)
            dbscan_labels = dbscan.fit_predict(coords_std)

            iso_forest = IsolationForest(contamination=contamination, random_state=42)
            iso_forest_labels = iso_forest.fit_predict(coords_std)

            n_neighbors = min(20, len(coords_std) - 1)

            lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination)

            lof_labels = lof.fit_predict(coords_std)

            ensemble_labels = []
            for i in range(len(coords_std)):
                votes = [dbscan_labels[i] == -1, iso_forest_labels[i] == -1, lof_labels[i] == -1]
                ensemble_labels.append(-1 if sum(votes) >= 2 else 1)

            subset_indices = subset.index[:len(ensemble_labels)]
            self.final_csv.loc[subset_indices, 'outlier_score'] = ensemble_labels

    def write_output_csvs(self):
        self.final_csv.to_csv("geo_csvs/output_csv/all_output.csv", index=False,
                              encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        print("Coordinates cleaned and files written.")
