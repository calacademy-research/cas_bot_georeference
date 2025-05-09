import pandas as pd
import requests
import json
import math


class CleanCoords:
    def __init__(self, geolocate_csv):
        self.geolocate_data = geolocate_csv

    def filter_lat_long_frame(self):
        """Only keeps lat/long columns from processing if they exist."""
        columns = [
            'lat_verbatim_1', 'long_verbatim_1',
            'lat_verbatim_2', 'long_verbatim_2',
            'lat_verbatim_3', 'long_verbatim_3',
        ]
        existing_columns = [col for col in columns if col in self.geolocate_data.columns]
        return self.geolocate_data[existing_columns].copy()

    def extract_coords_for_column_pair(self, coord_frame, lat_col, lon_col):
        """
        Converts latitude/longitude pair into a list of [lat, lon] lists,
        skipping any rows where lat or lon is NaN or unparseable.
        """
        coords = []
        for _, row in coord_frame.iterrows():
            try:
                lat = float(row[lat_col])
                lon = float(row[lon_col])
            except (ValueError, TypeError):
                # couldn't parse to float
                continue

            # drop NaNs
            if math.isnan(lat) or math.isnan(lon):
                continue

            coords.append([lat, lon])

        return coords

    def batch_query_gvs(
            self,
            coord_num: int,
            coord_frame: pd.DataFrame,
            api_url: str = "https://gvsapi.xyz/gvs_api.php",
            mode: str = "resolve",
            maxdist: float | None = 10,
            maxdistrel: float | None = 0.1
    ) -> pd.DataFrame | None:
        """
        Batch‑queries the GVS API by building a payload of the form:
        {
          "opts": { "mode": "...", "maxdist": ..., "maxdistrel": ... },
          "data": [ [lat1, lon1], [lat2, lon2], … ]
        }
        """

        # 1) pull out only the finite coords
        data = self.extract_coords_for_column_pair(
            coord_frame,
            f"lat_verbatim_{coord_num}",
            f"long_verbatim_{coord_num}"
        )
        if not data:
            print("No valid coordinates for batch", coord_num)
            return None

        # 2) build opts exactly as in R
        opts: dict[str, float | str] = {"mode": mode}
        if maxdist is not None:
            opts["maxdist"] = maxdist
        if maxdistrel is not None:
            opts["maxdistrel"] = maxdistrel

        payload = {
            "opts": opts,
            "data": data
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "charset": "UTF-8"
        }

        try:
            # 3) POST the JSON exactly as R does
            resp = requests.post(api_url, headers=headers, data=json.dumps(payload))
            resp.raise_for_status()
            return pd.DataFrame(resp.json())

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def clean_coords(self):
        """Loops through each lat_verbatim_X column and prints the GVS results."""
        coord_frame = self.filter_lat_long_frame()
        matches = sum(name.startswith("lat_verbatim_") for name in coord_frame.columns)

        for i in range(1, matches + 1):
            df = self.batch_query_gvs(coord_num=i, coord_frame=coord_frame)
            print(f"Results for lat_verbatim_{i}:\n", df)

