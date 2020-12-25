import os

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine

import constants


def connect_to_sim_db(engine: Engine = None) -> Session:
    if engine is None:
        engine = sa.create_engine(constants.SIMDB)
    LocalSession = sessionmaker(bind=engine)
    return LocalSession()


def print_bar():
    print('=' * os.get_terminal_size()[0])
