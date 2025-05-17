"""This file will be for post-processing cleaning of Lat/Long coordinates.
Occupying the last major step of the pipeline. Geolocate --> GVS --> Cleaning"""
import pandas as pd
class CleanCoords:
    def __init__(self, processed_csv):
        self.final_csv = processed_csv
        self.placeholder_function()

    def placeholder_function(self):
        self.final_csv.to_csv("geo_csvs/output_csv/final_output.csv", index=False, encoding="utf-8-sig")
        print("Coordinates Cleaned !")
