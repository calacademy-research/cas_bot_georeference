import csv
import os

class CleanCoords:
    def __init__(self, processed_csv, logger):
        self.final_csv = processed_csv
        self.coge_ready = None
        self.import_ready = None
        self.logger = logger
        self.conn = None
        self.logger.info("Initializing and cleaning coordinates...")
        self.initial_filter_results()
        self.drop_rename_columns()
        self.write_output_csvs()

    def initial_filter_results(self):
        self.final_csv['com_georef'] = False
        self.final_csv['gvs_county'] = self.final_csv['gvs_county'] + " County"
        print(list(self.final_csv.columns))
        missing_coords = self.final_csv['Geo_Lat'].isna() | self.final_csv['Geo_Lon'].isna()
        no_bels = (self.final_csv['Geo_Source'] != "bels")
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
            centroid_missing_flag |
            no_bels
        )

    def drop_rename_columns(self):
        """re-formats and drops columns from geolocate pipeline that are not necessary for
           Community geo-referencing"""

        reduced_final_csv = self.final_csv[['index', 'Final_Suggested_ID', 'Confidence', 'country', 'stateprovince',
                                            'county', 'locality', 'normalized_locality',
                                            'bels_match', 'Geo_Lat', 'Geo_Lon', 'Geo_UncertaintyM',
                                            'datum', 'com_georef']]

        self.import_ready = reduced_final_csv[~reduced_final_csv['com_georef']]

        self.coge_ready = reduced_final_csv[reduced_final_csv['com_georef']]

        self.coge_ready.rename(columns={'Final_Suggested_ID': 'catalogNumber',
                                        'Geo_Lat': 'decimalLatitude',
                                        'Geo_Lon': 'decimalLongitude',
                                        'Geo_UncertaintyM': 'coordinateUncertaintyInMeters'}, inplace=True)

        self.coge_ready.drop(['com_georef', 'bels_match'], axis=1, inplace=True)

        self.coge_ready['scientificName'] = 'Planta alba'

    def write_output_csvs(self):

        county = ("_".join(self.final_csv["county"].dropna().unique().astype(str))).lower()

        county = county.replace(" ", "_")

        self.coge_ready.to_csv(f"geo_csvs{os.path.sep}output_csv{os.path.sep}geo_coge_{county}.csv", index=False,
                                encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        self.import_ready.to_csv(f"geo_csvs{os.path.sep}output_csv{os.path.sep}geo_import_{county}.csv", index=False,
                                 encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        # optional full csv with not columns dropped , for diagnostic/debugging purposes.
        # self.final_csv.to_csv(f"geo_csvs{os.path.sep}output_csv{os.path.sep}full_csv_{county}.csv", index=False,
        #                         encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)

        print("Coordinates cleaned and files written.")
