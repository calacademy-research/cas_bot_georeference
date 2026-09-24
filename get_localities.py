import pandas as pd
import os
import argparse
from specify_db import SpecifyDb
import database_config as config

class GetLocalities:
    def __init__(self, output_csv, country, state=None, county=None):
        self.record_full = None
        self.specify_db_connection = SpecifyDb(db_config_class=config)
        self.county = (county or "").strip()
        self.state = (state or "").strip()
        self.country = country.strip()
        self.output_csv = output_csv
        self.fetch_localities()


    def fetch_localities(self):

        geography_full_name = ", ".join(
            part for part in (self.county, self.state, self.country) if part
        )

        query = f"""
            SELECT
                l.LocalityID,
                g.FullName,
                l.LocalityName AS locality,
                ce.StartDate AS collection_date,
                t.FullName AS taxonomic_name
            FROM locality AS l
            JOIN geography AS g
                ON g.GeographyID = l.GeographyID
            JOIN collectingevent AS ce
                ON ce.LocalityID = l.LocalityID
            JOIN collectionobject AS co
                ON co.CollectingEventID = ce.CollectingEventID
            JOIN determination AS d
                ON d.CollectionObjectID = co.CollectionObjectID
            JOIN taxon AS t
                ON t.TaxonID = d.TaxonID
            WHERE g.FullName = %s
                AND l.Lat1Text IS NULL
                AND d.IsCurrent = 1
        """

        rows = self.specify_db_connection.get_records(query, params=(geography_full_name,))

        col_names = ["LocalityID", "geography_full_name", "locality", "collection_date", "taxonomic_name"]

        self.record_full = pd.DataFrame(rows, columns=col_names)

        self.split_geography_columns()

        self.record_full.drop(columns="geography_full_name", inplace=True)

        self.record_full.to_csv(f"geo_csvs{os.path.sep}raw_localities{os.path.sep}{self.output_csv}.csv",
                                sep=",", quotechar='"')

    def split_geography_columns(self):
        parts = (
            self.record_full["geography_full_name"]
            .fillna("")
            .str.split(",")
            .str[::-1]
        )

        insert_at = self.record_full.columns.get_loc("LocalityID") + 1

        for offset, column in enumerate(("country", "state", "county")):
            self.record_full.insert(
                insert_at + offset,
                column,
                parts.str.get(offset).fillna("").str.strip(),

            )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the full geolocation pipeline.')
    parser.add_argument('-oc', '--output_csv', default=None, required=True, help="name of csv to output")
    parser.add_argument('-cn', '--country', default=None, required=True, help='Country field name')
    parser.add_argument('-st', '--state', default=None, help='State field name')
    parser.add_argument('-ct', '--county', default=None, help='County field name')
    args = parser.parse_args()
    GetLocalities(output_csv=args.output_csv, country=args.country, state=args.state, county=args.county)
