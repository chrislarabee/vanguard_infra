from pathlib import Path

import pytest
from sqlalchemy import create_engine, func as sa_func
from sqlalchemy.orm import sessionmaker

from gen_db import lib
from callcenter.app.db import models


@pytest.fixture
def sample_voters():
    return [
        models.Voter(ohvfid='001', first_name='Justin'),
        models.Voter(ohvfid='002', first_name='Travis'),
        models.Voter(ohvfid='003', first_name='Griffin'),
    ]


@pytest.fixture
def test_db(sample_voters):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False}
    )
    models.Base.metadata.create_all(engine)
    TestingSession = sessionmaker(engine)
    s = TestingSession()
    s.add_all(sample_voters)
    s.commit()
    yield s
    s.close()


def test_gen_call_data(test_db):
    lib.gen_call_data(test_db, 1/3)
    assert test_db.query(models.Call).count() == 3
    assert test_db.query(sa_func.sum(models.Call.call_result)).one()[0] == 1


def test_gen_populate_cenblocks():
    result = lib.gen_populate_cenblocks('oh_dist4')
    assert 'MAX(totalpop)' in result
    assert 'SUM(is_donor)' in result
    assert ' (blockgeoid,' in result
    assert ' FROM oh_dist4 GROUP BY blockgeoid;' in result


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