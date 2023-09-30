import os

from fire import Fire
import geopandas as gpd
import sqlalchemy as sa
from dotenv import load_dotenv

from gobosm import init_gob_table, load_gob, load_osm, compare


class GOBOSM:

    def __init__(self):
        load_dotenv()

    def gob(self, replace=False, work_dir="./work", num_processes=4):

        engine = sa.create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
        init_gob_table(engine, replace=replace)
        engine.dispose()

        gob_gdf = gpd.read_file("./data/gob_idx.geojson")\

        # TODO: remove this ############################
        gob_gdf = gob_gdf.sort_values("size_mb").head(1)
        ###############################################

        for _, row in gob_gdf.iterrows():
            load_gob(row["tile_url"], work_dir, num_processes)

        engine.execute("create index gob_geom_index on gob using gist (geometry)")

    def osm(self, pbf_url=None, work_dir="./work", slim=True):

        # TODO: remove this #####################################################
        pbf_url = "https://download.geofabrik.de/africa/mauritius-latest.osm.pbf"
        #########################################################################

        load_osm(pbf_url, work_dir, slim)

    def compare(self):
        compare()

if __name__ == "__main__":
    Fire(GOBOSM)