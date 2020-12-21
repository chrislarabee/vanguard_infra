import datastore.database as db


def test_create_table():
    expected = "CREATE TABLE test (col_a INTEGER, col_b VARCHAR, col_c FLOAT);"
    assert db.gen_create_table(
        'test',
        dict(col_a='INTEGER', col_b='VARCHAR', col_c='FLOAT')
    ) == expected


def test_gen_insert_table():
    expected = "INSERT INTO test (col_a, col_b, col_c) "
    assert  db.gen_insert_table(
        'test',
        dict(col_a='INTEGER', col_b='VARCHAR', col_c='FLOAT')
    ) == expected