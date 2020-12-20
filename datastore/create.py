from pathlib import Path

from .prepdata import PrepData
from . import lib


def setup_dirs():
    sim_db = Path('datastore/sim_db')
    sim_db.mkdir(exist_ok=True)


if __name__ == "__main__":
    setup_dirs()
    p = PrepData(lib.prep_raw_data, 10000)
    p.execute('oh_dist4.csv')
