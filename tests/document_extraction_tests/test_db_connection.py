import pytest
import duckdb
import pandas as pd
from utils.db_connection import DBConnection

@pytest.fixture
def db_connection():
    DBConnection._instances = {}
    db = DBConnection(":memory:")
    
    yield db

    DBConnection._instances = {}

def test_singleton_returns_same_instance(db_connection):
    """Test that DBConnection returns the same instance when initialized twice for same database"""
    
    db2 = DBConnection(":memory:")

    assert db_connection is db2


def test_different_paths_create_different_instances(db_connection):

    db2 = DBConnection("/tmp/test.db")

    assert db2 is not db_connection


def test_connect_establishes_connection(db_connection):

    assert db_connection._connection is None

    db_connection.connect()

    assert db_connection._connection is not None
    assert isinstance(db_connection._connection, duckdb.DuckDBPyConnection)


def test_query_returns_dataframe(db_connection):
    db_connection.execute("CREATE TABLE test (id INTEGER, string VARCHAR);")
    db_connection.execute("INSERT INTO test VALUES (1, 'boop'), (2, 'easy');")

    result = db_connection.query("SELECT * FROM test ORDER BY id;")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ['id', 'string']
    assert result.iloc[0]['string'] == 'boop'


def test_atomic_transaction_behaviour(db_connection):
    db_connection.execute('CREATE TABLE test_tmp (id INTEGER, string VARCHAR);')
    with db_connection as db:
        db.execute("INSERT INTO test_tmp VALUES (1, 'test1');")
        db.execute("INSERT INTO test_tmp VALUES (2, 'test2');")

    result = db_connection.query("SELECT * FROM test_tmp ORDER BY id;")

    assert len(result) == 2
    assert result.iloc[0]['string'] == 'test1'
    assert result.iloc[1]['string'] == 'test2'

def test_atomic_transaction_rollback(db_connection):
    db_connection.execute('CREATE TABLE test_tmp (id INTEGER, string VARCHAR);')
    with pytest.raises(SyntaxError):
        with db_connection as db:
            db.execute("INSERT INTO test_tmp VALUES (1, 'correct');")
            
            # Fail a second insert on purpose due to syntax error. Pass it to confirm the rollback
            db.execute("INVALID SQL;")

    result = db_connection.query("SELECT * FROM test_tmp ORDER BY id;")

    assert len(result) == 0