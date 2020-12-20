from pathlib import Path
import sqlite3

from .prepdata import PrepData
from . import constants, lib, database as db, util as u


def setup_dirs():
    sim_db = Path('datastore/sim_db')
    sim_db.mkdir(exist_ok=True)


if __name__ == "__main__":
    setup_dirs()
    p = PrepData(lib.prep_raw_data)
    p.execute('oh_dist4.csv')
    u.print_bar()
    print('Begin database build out...')
    conn = sqlite3.connect(constants.SIMDB.joinpath('datasets.db'))
    c = conn.cursor()
    print('Creating districts table...')
    c.execute(db.create_district_table())
    print('Creating census blocks (cenblocks) table...')
    c.execute(db.create_cenblocks_table())
    print('Creating voter table...')
    c.execute(db.create_voter_table())
    conn.commit()
    print('Table creation complete.')
    u.print_bar()
    print('Database build out complete.')
    conn.close()
