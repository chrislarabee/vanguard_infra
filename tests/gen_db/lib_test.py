from pathlib import Path
import shutil

import pytest
from sqlalchemy import create_engine, func as sa_func
from sqlalchemy.orm import sessionmaker
import pandas as pd

from gen_db import lib
from callcenter.app.db import models
from callcenter.app import constants


@pytest.fixture
def sample_voters():
    return [
        models.Voter(ohvfid='001', first_name='Justin'),
        models.Voter(ohvfid='002', first_name='Travis'),
        models.Voter(ohvfid='003', first_name='Griffin'),
    ]


@pytest.fixture
def sample_cenblocks():
    return [
        models.CensusBlock(blockgeoid=1, total_donors=50, totalpop=200),
        models.CensusBlock(blockgeoid=2, total_donors=5, totalpop=200),
        models.CensusBlock(blockgeoid=3, total_donors=100, totalpop=200),
    ]


@pytest.fixture
def test_db(sample_voters, sample_cenblocks):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False}
    )
    models.Base.metadata.create_all(engine)
    TestingSession = sessionmaker(engine)
    s = TestingSession()
    s.add_all(sample_voters)
    s.add_all(sample_cenblocks)
    s.commit()
    yield s
    s.close()


@pytest.fixture
def output_dir():
    p = Path('output')
    p.mkdir(exist_ok=True)
    yield p
    shutil.rmtree(p)


def test_gen_call_data(test_db):
    lib.gen_call_data(test_db, 1/3)
    assert test_db.query(models.Call).count() == 3
    assert test_db.query(sa_func.sum(models.Call.call_result)).one()[0] == 1


def test_prep_training_data(test_db, output_dir,  monkeypatch):
    monkeypatch.setattr(constants, 'TRAIN', output_dir)
    lib.prep_cenblock_training_data(test_db, batch_size=1)
    df = pd.read_csv(output_dir.joinpath('cenblocks.csv'))
    assert len(df) == 3
    assert df['donor_pct'].tolist() == [0.25, 0.025, 0.5]
    assert len(df.columns) == len(models.CensusBlock.gen_column_list()) + 1


def test_prep_training_data_w_sample_cap(test_db, output_dir, monkeypatch):
    monkeypatch.setattr(constants, 'TRAIN', output_dir)
    lib.prep_cenblock_training_data(test_db, num_samples=2, batch_size=1)
    df = pd.read_csv(output_dir.joinpath('cenblocks.csv'))
    assert len(df) == 2
    assert df['donor_pct'].tolist() == [0.25, 0.025]


def test_gen_populate_cenblocks():
    result = lib.gen_populate_cenblocks('oh_dist4')
    assert 'MAX(totalpop)' in result
    assert 'SUM(is_donor)' in result
    assert ' (blockgeoid,' in result
    assert ' FROM oh_dist4 GROUP BY blockgeoid;' in result
    assert ' id, ' not in result


def test_gen_populate_voters():
    result = lib.gen_populate_voters('oh_dist4')
    assert ' id, ' not in result


def test_gen_insert_table():
    expected = "INSERT INTO test (col_a, col_b, col_c) "
    assert lib.gen_insert_table(
        'test',
        dict(col_a='INTEGER', col_b='VARCHAR', col_c='FLOAT')
    ) == expected


def test_gen_select():
    expected = "SELECT SUM(col_a), MAX(col_b), MIN(col_c) FROM test;"
    assert lib.gen_select(
        'test',
        ['SUM(col_a)', 'MAX(col_b)', 'MIN(col_c)']
    ) == expected


def test_gen_select_w_groupby():
    expected = (
        "SELECT SUM(col_a), MAX(col_b), MIN(col_c) FROM test GROUP BY col_v, "
        "col_w;"
    )
    assert lib.gen_select(
        'test',
        ['SUM(col_a)', 'MAX(col_b)', 'MIN(col_c)'],
        ['col_v', 'col_w']
    ) == expected