import os
import argparse
import shutil
from pathlib import Path
import sqlite3

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .prepdata import PrepData
from app.db import models
from . import lib
import constants
import util as u

def setup_dirs(recreate=False):
    if recreate:
        shutil.rmtree(constants.SIM)
    constants.SIM.mkdir(exist_ok=True)


def build_out_db(source_table: str):
    print('Begin database build out...')
    u.print_bar()
    conn = sqlite3.connect(constants.SIMDB)
    c = conn.cursor()
    try:
        print('Populating census blocks (cenblocks) table...')
        c.execute(lib.gen_populate_cenblocks(source_table))
        conn.commit()
        print('Populating voters table...')
        c.execute(lib.gen_populate_voters(source_table))
        conn.commit()
        print('Table population complete.')
        u.print_bar()
    finally:
        conn.close()
    print('Database build out complete.')


def gen_and_populate_calls(session: Session):
    u.print_bar()
    print('Begin simulated call data generation...')
    u.print_bar()
    print('Clearing out any existing call data...')
    session.execute(sa.delete(models.Call))
    session.commit()
    print('Generating new simulated call data...')
    try:
        lib.gen_call_data(session, batch_size=10000)
    finally:
        session.close()
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
    
    engine = sa.create_engine(constants.SQL_ALCHEMY_SIMDB)
    
    if args.recreate in ['all', 'db']:
        print('Begin table creation.')
        u.print_bar()
        models.Base.metadata.drop_all(engine)
        models.Base.metadata.create_all(engine)
        print('Table creation complete.')
        u.print_bar()
        build_out_db(raw_file.stem)

    if args.recreate in ['all', 'call']:
        s = u.connect_to_sim_db(engine)
        gen_and_populate_calls(s)
