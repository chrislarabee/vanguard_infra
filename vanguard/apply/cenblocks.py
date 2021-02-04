import requests
import json

import pandas as pd
import sqlalchemy as sa

from ..db import constants, util as u, models


if __name__ == "__main__":
    engine = sa.create_engine(
        constants.SQL_ALCHEMY_SIMDB, connect_args=dict(check_same_thread=False)
    )
    session = u.connect_to_sim_db(engine)
    query = session.query(models.CensusBlock)
    df = pd.read_sql(query.statement, session.bind)
    df["totalpop"] = df["totalpop"].astype(int)
    url = "http://localhost:8501/v1/models/cenblocks:regress"
    data = df.to_dict("records")
    j = dict(examples=data)
    r = requests.post(url, data=json.dumps(j))
    if "results" in r.json().keys():
        df["rating"] = pd.Series(r.json()["results"])
        df["cenblock_rating"] = df.apply(
            lambda row: models.CenblockRating(
                blockgeoid=row["blockgeoid"], rating=row["rating"]
            ),
            axis=1
        )
        session.add_all(df["cenblock_rating"].values.tolist())
        session.commit()
    else:
        print(r.json())
    session.close()
    