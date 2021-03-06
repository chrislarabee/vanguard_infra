import math
import time
import json

from kafka import KafkaProducer
from sqlalchemy.orm import Session

from .db.models import Call
from .db import util as u


def call_stream_generator(db: Session, batch_size: int = 10000):
    call_ct = db.query(Call).count()
    batches = math.ceil(call_ct / batch_size)
    start = 1
    end = batch_size
    for _ in range(batches):
        yield db.query(Call).filter(Call.id >= start).filter(Call.id <= end).all()
        start += batch_size
        end += batch_size


def stream_calls(db: Session, batch_size: int = 10000, secs_btw: int = 2):
    producer = KafkaProducer(bootstrap_servers="kafka:9092")
    for i, call_batch in enumerate(call_stream_generator(db, batch_size), 1):
        print(f"Sending batch {i} to kafka server...")
        for j, call in enumerate(call_batch, 1):
            print(f"Streaming call {j}...", end="\r")
            b = json.dumps(dict(ohvfid=call.ohvfid, call_result=call.call_result))
            producer.send("incoming_calls", b.encode("utf-8"))
        time.sleep(secs_btw)


if __name__ == "__main__":
    db = u.connect_to_sim_db()
    stream_calls(db)
    db.close()
