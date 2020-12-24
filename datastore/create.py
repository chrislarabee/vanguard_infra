import os
import argparse
import shutil
from pathlib import Path
import sqlite3

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from .prepdata import PrepData
from . import constants, lib, database as db, util as u


def setup_dirs(recreate=False):
    if recreate:
        shutil.rmtree(constants.SIMDB)
    constants.SIMDB.mkdir(exist_ok=True)


def build_out_db(source_table: str):
    print('Begin database build out...')
    u.print_bar()
    conn = sqlite3.connect(constants.SIMDB.joinpath('datasets.db'))
    c = conn.cursor()
    try:
        print('Populating census blocks (cenblocks) table...')
        c.execute(db.gen_populate_cenblocks(source_table))
        conn.commit()
        print('Populating voters table...')
        c.execute(db.gen_populate_voters(source_table))
        conn.commit()
        print('Table population complete.')
        u.print_bar()
    finally:
        conn.close()
    print('Database build out complete.')


def gen_and_populate_calls(engine: Engine):
    u.print_bar()
    print('Begin simulated call data generation...')
    u.print_bar()
    Session = sessionmaker(bind=engine)
    s = Session()
    print('Clearing out any existing call data...')
    s.execute(sa.delete(db.Call))
    s.commit()
    print('Generating new simulated call data...')
    try:
        db.gen_call_data(s, batch_size=10000)
    finally:
        s.close()
    u.print_bar()
    print('Simulated call data generated.')

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
        default='n',
        help='Determines what parts, if any, of the simulated database '
             'to create from scratch (db=Build out the database, '
             'call=Re-generate the call data, all=Recreate everything, ' 
             'n=Do not re-create anything (this is the default)).'
    )

    args = parser.parse_args()

    raw_file = args.raw_file
    if not raw_file:
        raw_file = os.listdir('datastore/raw_data')[0]
    raw_file = Path(raw_file)
    setup_dirs(args.recreate == 'all')

    if args.recreate == 'all':
        p = PrepData(lib.prep_raw_data)
        p.execute(raw_file.stem)
        u.print_bar()
    
    engine = sa.create_engine('sqlite:///datastore/sim_db/datasets.db')
    
    if args.recreate in ['all', 'db']:
        print('Begin table creation.')
        u.print_bar()
        db.Base.metadata.drop_all(engine)
        db.Base.metadata.create_all(engine)
        print('Table creation complete.')
        u.print_bar()
        build_out_db(raw_file.stem)

    if args.recreate in ['all', 'call']:
        gen_and_populate_calls(engine)
