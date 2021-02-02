import pandas as pd
import sqlalchemy as sa
import tensorflow as tf

from components import constants

if __name__ == "__main__":
    model = tf.saved_model.load("models/cenblocks")
    engine = sa.create_engine(
        constants.SQL_ALCHEMY_SIMDB, connect_args=dict(check_same_thread=False)
    )
    
    for df in pd.read_ql()
