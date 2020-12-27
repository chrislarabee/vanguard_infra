import os
import argparse
import shutil
from pathlib import Path
import sqlite3

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd

from .prepdata import PrepData
from callcenter.app.db import models
from callcenter.app import util as u, constants
from . import lib


def setup_dirs(recreate=False):
    if recreate:
        shutil.rmtree(constants.SIM, onerror=FileNotFoundError)
        shutil.rmtree(constants.TRAIN, onerror=FileNotFoundError)
    constants.SIM.mkdir(exist_ok=True)
    constants.TRAIN.mkdir(exist_ok=True)


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


def gen_and_populate_calls(
        session: Session,
        pos_resp_rate: float = 0.1,
        num_samples: int = None,
        batch_size: int = 100000):
    u.print_bar()
    print('Begin simulated call data generation...')
    u.print_bar()
    print('Clearing out any existing call data...')
    session.execute(sa.delete(models.Call))
    session.commit()
    print('Generating new simulated call data...')
    try:
        lib.gen_call_data(
            session, 
            pos_resp_rate=pos_resp_rate,
            num_samples=num_samples,
            batch_size=batch_size)
    finally:
        session.close()
    u.print_bar()
    print('Simulated call data generated.')


def create_training_data(
        session: Session,
        num_samples: int = None,
        batch_size: int = 100000): 
    u.print_bar()
    print('Begin production of training data...')
    u.print_bar()
    print('Clearing out any existing census block rating training data...')
    p = constants.TRAIN.joinpath('cenblocks.csv')
    if p.exists:
        p.unlink()
    print('Preparing new census block rating training data...')
    try:
        lib.prep_cenblock_training_data(
            session,
            num_samples=num_samples,
            batch_size=batch_size
        )
    finally:
        session.close()
    print('Census block rating training data prep complete.')


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
             'to create from scratch ('
             'db=Build out the database, '
             'call=Re-generate the call data, '
             'train=Re-prep the training data, '
             'all=Recreate everything, ' 
             'n=Do not re-create anything (this is the default)).'
    )

    parser.add_argument(
        '--batch_size', '-b',
        type=int,
        default=100000,
        help='The batch size for various steps of the data preparation '
             'stage. Default is 100,000'
    )

    parser.add_argument(
        '--num_samples', '-n',
        type=int,
        help='The # of call/census samples to generate. Default is '
             'the # of records in the voter/cenblocks table.'
    )

    parser.add_argument(
        '--pos_resp_rate', '-p',
        type=float,
        default=0.1,
        help='The percentage of call samples to be generated as positive '
             'responses. Default is 0.1.'
    )

    parser.add_argument(
        '--manual_header', '-m',
        help='The name of a csv file in datastore/templates to pull the '
             'header from. Useful if your raw data file has no header row.'
    )

    args = parser.parse_args()

    raw_file = args.raw_file
    if not raw_file:
        raw_file = os.listdir('datastore/raw_data')[0]
    raw_file = Path(raw_file)
    setup_dirs(args.recreate == 'all')

    if args.recreate == 'all':
        h = None
        if args.manual_header:
            h = pd.read_csv(f'datastore/templates/{args.manual_header}')
            h = h['column'].tolist()
        p = PrepData(
            lib.prep_raw_data, 
            batch_size=args.batch_size,
            manual_header=h
        )
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

    if args.recreate in ['all', 'call', 'train']:
        s = u.connect_to_sim_db(engine)
        if args.recreate in ['all', 'call']:
            gen_and_populate_calls(
                s,
                pos_resp_rate=args.pos_resp_rate,
                num_samples=args.num_samples,
                batch_size=args.batch_size
            )
        if args.recreate in ['all', 'train']:
            create_training_data(
                s,
                num_samples=args.num_samples,
                batch_size=args.batch_size
            )
            

