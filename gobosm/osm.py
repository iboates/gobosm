import os
import subprocess as sp


def _download(url, dest):

    sp.run(["wget", "-O", dest, url])


def _osm2pgsql(pbf, slim=False):

    args = [
        "osm2pgsql",
        "-d", os.getenv("DB_NAME"),
        "-U", os.getenv("DB_USER"),
        "-H", os.getenv("DB_HOST"),
        "-P", str(os.getenv("DB_PORT")),
        "-l",  # EPSG 4326
        "-O", "flex",
        "-S", "./flex/buildings.lua",
        pbf,
    ]
    if slim:
        args = [*args[:-1], "--slim", args[-1]]
    sp.run(args, env={**os.environ.copy(), "PGPASSWORD": os.getenv("DB_PASS")})


def load_osm(pbf_download_link, work_dir, slim):

    _download(pbf_download_link, f"{work_dir}/osm.pbf")
    _osm2pgsql(f"{work_dir}/osm.pbf", slim=slim)
