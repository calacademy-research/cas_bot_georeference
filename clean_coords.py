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


    def write_output_csvs(self):
        self.final_csv.to_csv("geo_csvs/output_csv/all_output.csv", index=False,
                              encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        print("Coordinates cleaned and files written.")
