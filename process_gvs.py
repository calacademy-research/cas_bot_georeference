import pandas as pd
import requests
import json
import logging

class GVSProcess:
    def __init__(self, geocoded_csv):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
        self.input_csv = geocoded_csv.reset_index(drop=True)
        self.merged_df = None
        self.process_csv_gvs()

    def filter_lat_long_frame(self):
        """Extract Geo_Lat and Geo_Lon if available and valid."""
        required_columns = ['Geo_Lat', 'Geo_Lon']
        if not all(col in self.input_csv.columns for col in required_columns):
            raise ValueError("Geo_Lat and Geo_Lon must exist in input data")

        df = self.input_csv[required_columns].dropna()
        df = df[(df['Geo_Lat'].apply(self._is_number)) & (df['Geo_Lon'].apply(self._is_number))]
        return df.drop_duplicates().copy()

    def _is_number(self, x):
        try:
            float(x)
            return True
        except (ValueError, TypeError):
            return False

    def batch_query_gvs(
            self,
            coords_df: pd.DataFrame,
            api_url: str = "https://gvsapi.xyz/gvs_api.php",
            mode: str = "resolve",
            maxdist: float | None = 10,
            maxdistrel: float | None = 0.1,
            chunk_size: int = 100
    ) -> pd.DataFrame | None:
        """Query GVS API in chunks and return a concatenated results DataFrame with exact float coordinate mapping."""
        if coords_df.empty:
            self.logger.warning("No valid coordinates to query.")
            return None

        all_results = []
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "charset": "UTF-8"
        }
        for i in range(0, len(coords_df), chunk_size):
            chunk_df = coords_df.iloc[i:i + chunk_size].copy()
            chunk_coords = chunk_df[["Geo_Lat", "Geo_Lon"]].values.tolist()

            payload = {
                "opts": {"mode": mode, "maxdist": maxdist, "maxdistrel": maxdistrel},
                "data": chunk_coords
            }

            try:
                resp = requests.post(api_url, headers=headers, data=json.dumps(payload))
                resp.raise_for_status()
                result = pd.DataFrame(resp.json())

                if result.empty:
                    self.logger.warning(f"No GVS results returned for chunk {i // chunk_size + 1}")
                    continue

                result.rename(columns={'latitude_verbatim': 'Geo_Lat', 'longitude_verbatim': 'Geo_Lon'}, inplace=True)

                for col_name in ["Geo_Lat", "Geo_Lon"]:
                    chunk_df[col_name] = pd.to_numeric(chunk_df[col_name], errors="coerce")
                    result[col_name] = pd.to_numeric(result[col_name], errors="coerce")

                merged = pd.merge(
                    chunk_df,
                    result,
                    on=["Geo_Lat", "Geo_Lon"],
                    suffixes=('', '_gvs')
                )

                all_results.append(merged)

            except requests.exceptions.RequestException as e:
                self.logger.error(f"GVS API error on chunk {i // chunk_size + 1}: {e}")
                continue

        if not all_results:
            return None

        return pd.concat(all_results, ignore_index=True)

    def process_csv_gvs(self):
        """Runs GVS geocoding and merges results back into original DataFrame using lat/lon."""
        coord_frame = self.filter_lat_long_frame()
        gvs_result_df = self.batch_query_gvs(coord_frame)

        if gvs_result_df is None:
            self.logger.error("No data returned from GVS API.")
            return

        # Coerce coordinate columns to numeric
        for df in [self.input_csv, gvs_result_df]:
            if df is not None:
                for col in ['Geo_Lat', 'Geo_Lon']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        gvs_result_df.rename(
            columns={'country': 'gvs_country', 'state': 'gvs_state', 'county': 'gvs_county'},
            inplace=True
        )

        # Merge on Geo_Lat and Geo_Lon
        self.merged_df = pd.merge(
            self.input_csv,
            gvs_result_df,
            how="left",
            on=["Geo_Lat", "Geo_Lon"],
            suffixes=('', '_gvs')
        )
        return self.merged_df
