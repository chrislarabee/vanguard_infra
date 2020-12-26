from pathlib import Path

DSTORE = Path('datastore')
TRAIN = DSTORE.joinpath('batch_train')
RAW = DSTORE.joinpath('raw_data')
SIM = DSTORE.joinpath('sim_db')

SIMDB = f"{SIM.joinpath('datasets.db')}"
SQL_ALCHEMY_SIMDB = f"sqlite:///{SIMDB}"