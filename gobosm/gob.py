import multiprocessing as mp
import gzip
import os
import shutil
import hashlib
import sys

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
from shapely.wkt import loads as wkt_loads
import numpy as np
from tqdm import tqdm
import requests
import warnings


def init_gob_table(engine, replace=False):

    if replace:
        x = input(f"WARNING! You are about to remove all the contents on database `{str(engine.url).split('@')[1]}`. Are you sure? (y/N): ")
        if x != "y":
            sys.exit(0)

    if replace:
        engine.execute("""drop table gob;""")

    engine.execute("""create table if not exists gob (
                        id serial primary key,
                        latitude float,
                        longitude float,
                        area_in_meters float,
                        confidence float,
                        geometry geometry(point, 4326))""")


def _download_and_extract(url, work_dir):

    hash = hashlib.md5(url.encode('utf-8'))

    os.makedirs(f"{work_dir}/gob", exist_ok=True)
    # Download the file
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Save the downloaded file
    with open(f"{work_dir}/gob/temp_{hash}.gz", "wb") as f:
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        progress_bar = tqdm(total=total_size, unit="B", unit_scale=True)

        for data in response.iter_content(block_size):
            f.write(data)
            progress_bar.update(len(data))

    # Extract the file
    with gzip.open(f"{work_dir}/gob/temp_{hash}.gz", "rb") as f_in:
        with open(f"{work_dir}/gob/temp_{hash}.csv", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Remove the downloaded .gz file
    os.remove(f"{work_dir}/gob/temp_{hash}.gz")

    return f"{work_dir}/gob/temp_{hash}.csv"


def _add_gdf(params):

    buildings_gdf = params[0]
    with warnings.catch_warnings():
        # complains about centroids being calculated in 4326, shut up nerd
        warnings.simplefilter("ignore")
        buildings_gdf.geometry = buildings_gdf.geometry.centroid

    engine = sa.create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    buildings_gdf.to_postgis("gob",
                             engine,
                             if_exists="append")
    engine.dispose()


def load_gob(gob_url, work_dir, num_processes):

    gob_csv = _download_and_extract(gob_url, work_dir)

    df = pd.read_csv(gob_csv)
    geometry = df['geometry'].apply(wkt_loads)
    buildings_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=4326)
    buildings_gdf.drop([c for c in buildings_gdf if c != buildings_gdf.geometry.name], axis="columns", inplace=True)

    buildings_gdfs = np.array_split(buildings_gdf, num_processes * 100)
    params = [(buildings_gdf, ) for buildings_gdf in buildings_gdfs]

    with mp.Pool(num_processes) as pool:
        pbar = tqdm(total=len(params), desc=f"Google Open Buildings")
        for _ in pool.imap_unordered(_add_gdf, params, chunksize=1):
            pbar.update(1)

