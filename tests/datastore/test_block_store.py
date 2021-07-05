from bami.datastore.block_store import LMDBLockStore
import pytest


@pytest.fixture
def lmdb_store(tmpdir):
    tmp_val = tmpdir
    path = str(tmp_val)
    print(path)
    db = LMDBLockStore(path)
    yield db
    db.close()
    tmp_val.remove()


def test_hot_start(tmpdir):
    test_key = 123123123123123
    test_blob = b"lopo1"
    tmp_val = tmpdir
    path = str(tmp_val)
    db = LMDBLockStore(path)

    db.add_transaction(test_key, test_blob)
    res = db.get_transaction_by_hash(test_key)
    assert res == test_blob
    db.close()

    db2 = LMDBLockStore(path)
    res = db2.get_transaction_by_hash(test_key)
    assert res == test_blob
    db2.close()
    tmp_val.remove()
