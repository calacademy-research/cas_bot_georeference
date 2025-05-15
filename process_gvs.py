import pandas as pd
import requests
import json
import math

class GVSProcess:
    def __init__(self, input_csv="geo_csvs/output_csv/test_geo_output.csv"):
        self.input_csv = input_csv
        self.geolocate_data = pd.read_csv(input_csv)
        self.process_csv_gvs()

    def filter_lat_long_frame(self):
        """Extract Geo_Lat and Geo_Lon if available and valid."""
        required_columns = ['Geo_Lat', 'Geo_Lon']
        if not all(col in self.geolocate_data.columns for col in required_columns):
            raise ValueError("Geo_Lat and Geo_Lon must exist in input data")

        df = self.geolocate_data[required_columns].dropna()
        df = df[(df['Geo_Lat'].apply(lambda x: self._is_number(x))) &
                (df['Geo_Lon'].apply(lambda x: self._is_number(x)))]
        return df.drop_duplicates().copy()

    def _is_number(self, x):
        try:
            float(x)
            return True
        except (ValueError, TypeError):
            return False

    def extract_coords(self, coord_frame):
        """Extracts list of [lat, lon] pairs."""
        return [[float(row['Geo_Lat']), float(row['Geo_Lon'])] for _, row in coord_frame.iterrows()]

    def batch_query_gvs(
        self,
        coords: list[list[float]],
        api_url: str = "https://gvsapi.xyz/gvs_api.php",
        mode: str = "resolve",
        maxdist: float | None = 10,
        maxdistrel: float | None = 0.1
    ) -> pd.DataFrame | None:
        """Query GVS API and return full DataFrame."""
        if not coords:
            print("No valid coordinates to query.")
            return None

        payload = {
            "opts": {"mode": mode, "maxdist": maxdist, "maxdistrel": maxdistrel},
            "data": coords
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "charset": "UTF-8"
        }

        try:
            resp = requests.post(api_url, headers=headers, data=json.dumps(payload))
            resp.raise_for_status()
            result = pd.DataFrame(resp.json())

            # Re-attach lat/lon so we can merge
            result["Geo_Lat"] = [lat for lat, _ in coords]
            result["Geo_Lon"] = [lon for _, lon in coords]
            return result

        except requests.exceptions.RequestException as e:
            print(f"GVS API error: {e}")
            return None

    def process_csv_gvs(self):
        """Queries GVS API and merges results back into original dataset."""
        coord_frame = self.filter_lat_long_frame()
        coords = self.extract_coords(coord_frame)
        gvs_result_df = self.batch_query_gvs(coords)

        if gvs_result_df is None:
            print("No data returned from GVS API.")
            return

        # Merge full GVS output on Geo_Lat and Geo_Lon
        merged_df = pd.merge(
            self.geolocate_data,
            gvs_result_df,
            how="left",
            on=["Geo_Lat", "Geo_Lon"]
        )

        print("Merged result preview:\n", merged_df.head())
        merged_df.to_csv("geo_csvs/output_csv/gvs_merged_output.csv", index=False)
