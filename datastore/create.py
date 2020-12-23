import os
import argparse
import shutil
from pathlib import Path
import sqlite3

import sqlalchemy as sa

from .prepdata import PrepData
from . import constants, lib, database as db, util as u


def setup_dirs(recreate=False):
    if recreate:
        shutil.rmtree(constants.SIMDB)
    constants.SIMDB.mkdir(exist_ok=True)


def build_out_db(source_table: str):
    print('Begin database build out...')
    u.print_bar()
    print('Creating tables...')
    engine = sa.create_engine('sqlite:///datastore/sim_db/datasets.db')
    db.Base.metadata.create_all(engine)
    print('Table creation complete.')
    u.print_bar()
    conn = sqlite3.connect(constants.SIMDB.joinpath('datasets.db'))
    c = conn.cursor()
    print('Populating census blocks (cenblocks) table...')
    c.execute(db.gen_populate_cenblocks(source_table))
    conn.commit()
    print('Populating voters table...')
    c.execute(db.gen_populate_voters(source_table))
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
    raw_file = Path(raw_file)
    setup_dirs(args.recreate)

    if args.recreate:
        p = PrepData(lib.prep_raw_data)
        p.execute(raw_file.stem)
        u.print_bar()
    
    build_out_db(raw_file.stem)
