import os
import argparse
import shutil
from pathlib import Path
import sqlite3

from .prepdata import PrepData
from . import constants, lib, database as db, util as u


def setup_dirs(recreate=False):
    sim_db = Path('datastore/sim_db')
    if recreate:
        shutil.rmtree(sim_db)
    sim_db.mkdir(exist_ok=True)


def build_out_db():
    print('Begin database build out...')
    u.print_bar()
    conn = sqlite3.connect(constants.SIMDB.joinpath('datasets.db'))
    c = conn.cursor()
    print('Creating districts table...')
    c.execute(db.gen_create_table('districts', db.district_cols))
    print('Creating census blocks (cenblocks) table...')
    c.execute(db.gen_create_table('cenblocks', db.cenblocks_cols))
    print('Creating voter table...')
    c.execute(db.gen_create_table('voters', db.voter_cols))
    conn.commit()
    print('Table creation complete.')
    u.print_bar()
    print('Populating census blocks (cenblocks) table...')
    c.execute(db.gen_populate_cenblocks())
    conn.commit()
    print('Table population complete.')
    u.print_bar()
    print('Database build out complete.')
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        'Create a simulated database from the target raw_data.'
    )

    parser.add_argument(
        '--raw_file', '-r',
        help='The name of the file in datastore/raw_data to build from. '
             'Default is the first file in raw_data (alphabetically).'
    )

    parser.add_argument(
        '--recreate', '-c', 
        type=bool,
        default=False,
        help='If True, delete the existing sim_db directory and recreate '
             'from scratch. Default is False.'
    )

    args = parser.parse_args()

    raw_file = args.raw_file
    if not raw_file:
        raw_file = os.listdir('datastore/raw_data')[0]

    setup_dirs(args.recreate)

    if args.recreate:
        p = PrepData(lib.prep_raw_data)
        p.execute(raw_file)
        u.print_bar()
    
    build_out_db()
