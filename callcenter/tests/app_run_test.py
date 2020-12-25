import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import run
from app.db import models

@pytest.fixture
def sample_calls():
    return [
        models.Call(ohvfid='001', call_result=0),
        models.Call(ohvfid='002', call_result=1),
        models.Call(ohvfid='003', call_result=0),
        models.Call(ohvfid='004', call_result=0),
        models.Call(ohvfid='005', call_result=1),
    ]


@pytest.fixture
def test_db(sample_calls):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False}
    )
    models.Base.metadata.create_all(engine)
    TestingSession = sessionmaker(engine)
    s = TestingSession()
    s.add_all(sample_calls)
    s.commit()
    yield s
    s.close()


def test_call_stream_generator(test_db):
    x = run.call_stream_generator(test_db, 1)
    a = next(x)
    assert len(a) == 1
    assert a[0].ohvfid == '001'
    a = next(x)
    assert len(a) == 1
    assert a[0].ohvfid == '002'
    
