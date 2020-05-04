import config
import etl
import psycopg2
import pytest
import re
from unittest.mock import MagicMock, Mock, PropertyMock, patch, call


# Keep it simple, make sure our tests don't rely on a config
config.db = {}

whitespace_re = re.compile(r'[\s\n\r]+', re.MULTILINE)
def canonicalize(val):
    """ Remove excess whitespace and use lower case
    """
    return whitespace_re.sub(' ', val).lower().strip()


def get_mock_cursor():
    cursor = Mock()
    cursor.execute=MagicMock()
    cursor.fetchall=MagicMock(return_value=[])
    cursor.close=MagicMock()
    return cursor


def mock_connection(cursor):
    """ Replace the context-manager-enabled connect() function
    """
    ctx = Mock()
    ctx.__enter__ = MagicMock(return_value=(Mock(), cursor))
    ctx.__exit__ = MagicMock()

    etl.connect = MagicMock(return_value=ctx)
    return ctx


@pytest.fixture
def mock_psycopg2():
    def conn(**kwargs):
        if kwargs.get("dbname")=='error':
            raise Exception()
        else:
            connection = Mock()
            connection.cursor = MagicMock(return_value=get_mock_cursor())
            connection.close = MagicMock()
            connection.commit = MagicMock()
            return connection

    psycopg2.connect = Mock()
    psycopg2.connect.side_effect = conn

    return psycopg2


@pytest.mark.parametrize("db, dsn, error",
                         [
                             ("test", "test", False),
                             ("error", "error", True)
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
            mock_psycopg2.connect.assert_called_once_with(dbname=dsn)
            conn.cursor.assert_called_once_with()
        conn.commit.assert_called_once_with()
        curs.close.assert_called_once_with()
        conn.close.assert_called_once_with()


def test_get_columns():
    cursor = get_mock_cursor()

    ctx = mock_connection(cursor)

    result = etl.get_columns(etl.Relation("test", "stest", "ttest"))

    ctx.__enter__.assert_called_once()

    etl.connect.assert_called_once_with("test")
    cursor.execute.assert_called_once()
    cursor.fetchall.assert_called_once()
    assert result == []


def test_make_sure_table_exists():
    cursor = get_mock_cursor()

    ctx = mock_connection(cursor)

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


def test_get_last_uploaded_time():
    cursor = get_mock_cursor()
    cursor.fetchone=MagicMock(return_value=[123])

    ctx = mock_connection(cursor)

    result = etl.get_last_uploaded_time(
        etl.Relation("foo", "bar", "baz"),
        etl.Relation("test", "public", "log_mock")
    )

    ctx.__enter__.assert_called_once()

    etl.connect.assert_called_once_with("test")
    cursor.execute.assert_called_once()
    assert len(cursor.execute.call_args.args) == 2
    assert canonicalize(
            '''SELECT loaded FROM "public"."log_mock" WHERE schema_name=%s AND relation_name=%s;'''
        ) == canonicalize(
            cursor.execute.call_args.args[0]
        )
    assert cursor.execute.call_args.args[1] == ('bar', 'baz')
    cursor.fetchone.assert_called_once()
    assert result == 123


def test_copy():
    cursor = get_mock_cursor()
    type(cursor).description=PropertyMock(return_value=[("foo_column",)])
    cursor.executemany=MagicMock()
    data = [("mock_value",)]
    cursor.fetchall=MagicMock(return_value=data)

    ctx = mock_connection(cursor)

    result = etl.copy(
        etl.Relation("src", "src_sch", "src_rel"),
        etl.Relation("tgt", "tgt_sch", "tgt_rel"),
        "mock_id",
        "mock_tstz_field",
        "mock_datetime"
    )

    ctx.__enter__.assert_has_calls([call(), call()])

    etl.connect.assert_has_calls([call("src"), call("tgt")])
    cursor.execute.assert_called_once()
    assert len(cursor.execute.call_args.args) == 2
    assert canonicalize(
            '''SELECT * FROM "src_sch"."src_rel" WHERE "mock_tstz_field" <@ DATERANGE(%s::DATE, NULL, '[)');'''
        ) == canonicalize(
            cursor.execute.call_args.args[0]
        )
    assert cursor.execute.call_args.args[1] == ("mock_datetime", )

    cursor.fetchall.assert_called_once()

    cursor.executemany.assert_called_once()
    assert len(cursor.executemany.call_args.args) == 2
    assert canonicalize(
            '''INSERT INTO "tgt_sch"."tgt_rel" ("foo_column") VALUES (%s) ON CONFLICT ("mock_id") DO NOTHING;'''
        ) == canonicalize(
            cursor.executemany.call_args.args[0]
        )
    assert cursor.executemany.call_args.args[1] == data


def test_log_upload():
    cursor = get_mock_cursor()
    cursor.fetchone=MagicMock(return_value=[123])

    ctx = mock_connection(cursor)

    result = etl.log_upload(
        etl.Relation("foo", "bar", "baz"),
        etl.Relation("test", "public", "log_mock"),
        "mock_datetime"
    )

    ctx.__enter__.assert_called_once()

    etl.connect.assert_called_once_with("test")
    assert len(cursor.execute.call_args_list) == 2

    delete_call, insert_call = cursor.execute.call_args_list

    assert len(delete_call.args) == 2
    assert len(insert_call.args) == 2

    assert canonicalize(
            '''DELETE FROM "public"."log_mock" WHERE schema_name=%s AND relation_name=%s;'''
        ) == canonicalize(
            delete_call.args[0]
        )
    assert delete_call.args[1] == ('bar', 'baz')
    
    assert canonicalize(
            '''INSERT INTO "public"."log_mock" (schema_name, relation_name, loaded) VALUES (%s, %s, %s);'''
        ) == canonicalize(
            insert_call.args[0]
        )
    assert insert_call.args[1] == ('bar', 'baz', 'mock_datetime')
