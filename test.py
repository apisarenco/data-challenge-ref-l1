import etl
import psycopg2
import pytest
import re
from unittest.mock import MagicMock, Mock, patch


whitespace_re = re.compile(r'[\s\n\r]+', re.MULTILINE)
def canonicalize(val):
    """ Remove excess whitespace and use lower case
    """
    return whitespace_re.sub(' ', val).lower().strip()


def get_mock_cursor():
    cursor = Mock()
    cursor.execute=MagicMock()
    cursor.fetchall=MagicMock(return_value=[])
    cursor.commit=MagicMock()
    cursor.close=MagicMock()
    return cursor


def get_mock_context(cursor):
    ctx = Mock()
    ctx.__enter__ = MagicMock(return_value=(Mock(), cursor))
    ctx.__exit__ = MagicMock()

    etl.connect = MagicMock(return_value=ctx)
    return ctx


@pytest.fixture
def mock_psycopg2():
    def conn(dsn):
        if dsn=='dbname=error':
            raise Exception()
        else:
            connection = Mock()
            connection.cursor = MagicMock(return_value=get_mock_cursor())
            connection.close = MagicMock()
            return connection

    psycopg2.connect = Mock()
    psycopg2.connect.side_effect = conn

    return psycopg2


@pytest.mark.parametrize("db, dsn, error",
                         [
                             ("test", "dbname=test", False),
                             ("error", "dbname=error", True)
                         ])
def test_connect(mock_psycopg2, db, dsn, error):
    if error:
        with pytest.raises(Exception):
            with etl.connect(db) as (conn, curs):
                mock_psycopg2.connect.assert_called_once_with(dsn)
                assert conn is None
                assert curs is None
    else:
        with etl.connect(db) as (conn, curs):
            mock_psycopg2.connect.assert_called_once_with(dsn)
            conn.cursor.assert_called_once_with()
        curs.commit.assert_called_once_with()
        curs.close.assert_called_once_with()
        conn.close.assert_called_once_with()


def test_get_columns():
    cursor = get_mock_cursor()

    ctx = get_mock_context(cursor)

    result = etl.get_columns(etl.Relation("test", "stest", "ttest"))

    ctx.__enter__.assert_called_once()

    etl.connect.assert_called_once_with("test")
    cursor.execute.assert_called_once()
    cursor.fetchall.assert_called_once()
    assert result == []


def test_make_sure_table_exists():
    cursor = get_mock_cursor()

    ctx = get_mock_context(cursor)

    result = etl.make_sure_table_exists(
        etl.Relation("test", "public", "mock"),
        [
            etl.Column("test", "TEXT"),
            etl.Column("test2", "INTEGER")
        ],
        "test"
    )

    ctx.__enter__.assert_called_once()

    etl.connect.assert_called_once_with("test")
    cursor.execute.assert_called_once()
    assert len(cursor.execute.call_args.args) == 1
    assert canonicalize(
        'CREATE TABLE IF NOT EXISTS "public"."mock" ( "test" text unique, "test2" integer );'
        ) == canonicalize(
            cursor.execute.call_args.args[0]
        )
