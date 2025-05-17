import pandas as pd

def rename_drop_columns(bels_csv: pd.DataFrame):
    print(bels_csv.columns)
    bels_csv.drop(['bels_interpreted_countrycode', 'bels_matchwithcoords',
                   'bels_matchverbatimcoords', 'bels_matchsanscoords',
                   'bels_georeferencedby', 'bels_georeferenceddate',
                   'bels_georeferenceprotocol', 'bels_georeferencesources',
                   'bels_georeferenceremarks', 'bels_georeference_score',
                   'bels_georeference_source','bels_best_of_n_georeferences',
                   'bels_match_type'], inplace=True, axis=1)

    bels_csv.rename({'bels_decimallatitude': 'latitude', 'bels_decimallongitude': 'longitude', 'bels_geodeticdatum': 'datum',
                     'bels_coordinateuncertaintyinmeters': 'coordinate_uncertainty_meters'}, inplace=True, axis=1)

    bels_csv['bels_match'] = bels_csv['latitude'].notna()

    bels_csv['county'] = bels_csv['county'].astype(str).str.strip() + " County"

    # for testing purposes
    bels_csv = bels_csv.sample(n=50, random_state=42)

    return bels_csv

