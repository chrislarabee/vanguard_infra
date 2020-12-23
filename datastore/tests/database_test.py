import datastore.database as db


def test_gen_populate_cenblocks():
    result = db.gen_populate_cenblocks('oh_dist4')
    assert 'MAX(totalpop)' in result
    assert 'SUM(is_donor)' in result
    assert ' (blockgeoid,' in result
    assert ' FROM oh_dist4 GROUP BY blockgeoid;' in result


def test_gen_populate_voters():
    result = db.gen_populate_voters('oh_dist4')
    assert ' id, ' not in result


def test_gen_insert_table():
    expected = "INSERT INTO test (col_a, col_b, col_c) "
    assert  db.gen_insert_table(
        'test',
        dict(col_a='INTEGER', col_b='VARCHAR', col_c='FLOAT')
    ) == expected


def test_gen_select():
    expected = "SELECT SUM(col_a), MAX(col_b), MIN(col_c) FROM test;"
    assert db.gen_select(
        'test',
        ['SUM(col_a)', 'MAX(col_b)', 'MIN(col_c)']
    ) == expected


def test_gen_select_w_groupby():
    expected = (
        "SELECT SUM(col_a), MAX(col_b), MIN(col_c) FROM test GROUP BY col_v, "
        "col_w;"
    )
    assert db.gen_select(
        'test',
        ['SUM(col_a)', 'MAX(col_b)', 'MIN(col_c)'],
        ['col_v', 'col_w']
    ) == expected