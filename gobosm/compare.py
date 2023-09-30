import os

import sqlalchemy as sa


def compare():
    engine = sa.create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    engine.execute(open("sql/cluster_osm.sql").read())
    engine.execute(open("sql/cluster_gob.sql").read())
    engine.dispose()
