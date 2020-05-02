import contextlib
from datetime import datetime
from functools import namedtuple
import psycopg2
from typing import List, Union


@contextlib.contextmanager
def connect(db: str):
    conn, cursor = None, None
    try:
        conn = psycopg2.connect(f"dbname={db}")
        cursor = conn.cursor()
        yield conn, cursor
    finally:
        if cursor:
            cursor.commit()
            cursor.close()
        if conn:
            conn.close()


Column = namedtuple("Column", ("name", "type"))
Relation = namedtuple("Relation", ("db", "schema", "name"))


def get_columns(rel: Relation):
    columns = []
    with connect(rel.db) as (_, curs):
        curs.execute(f"""
                     SELECT
                        column_name,
                        data_type
                     FROM information_schema.columns
                     WHERE
                        table_schema = %s
                        AND table_name = %s;
                     """, (rel.schema, rel.name))
        for (column, datatype) in curs.fetchall():
            columns.append(Column(column, datatype))
    return columns


def make_sure_table_exists(rel: Relation, columns: List[Column], key):
    with connect(rel.db) as (_, curs):
        cols_ddl = ',\n                         '.join(
            [f'"{col.name}" {col.type} {"UNIQUE" if key==col.name else ""}' for col in columns]
        )
        curs.execute(f"""
                     CREATE TABLE IF NOT EXISTS "{rel.schema}"."{rel.name}" (
                         {cols_ddl}
                     );
                     """)


def get_last_uploaded_time(rel: Relation, log: Relation) -> Union[datetime, None]:
    with connect(log.db) as (_, curs):
        curs.execute(f"""
                     SELECT loaded
                     FROM "{log.schema}"."{log.name}"
                     WHERE schema_name=%s AND relation_name=%s
                     """, (rel.schema, rel.name))
        row = curs.fetchone()
        if row:
            return row[0]


def copy(src: Relation, tgt: Relation, key_field: str, tstz_field: str, last_upload: datetime):
    with connect(src.db) as (_, curs):
        curs.execute(f"""
                     SELECT *
                     FROM "{src.schema}"."{src.name}"
                     WHERE "{tstz_field}" <@ TSTZRANGE(NULL, %s, '()')
                     """, (last_upload, ))

        columns = '", "'.join(desc[0] for desc in curs.description)
        placeholders = ', '.join(['%s'] * len(curs.description))

        insert_query = f"""
        INSERT INTO "{tgt.schema}"."{tgt.name}"
        ("{columns}")
        VALUES ({placeholders})
        """
        with connect(tgt.db) as (_, tgt_curs):
            tgt_curs.executemany(insert_query, curs.fetchall())


def log_upload(rel: Relation, log: Relation, last_upload: datetime):
    with connect(log.db) as (_, curs):
        curs.execute(f"""
                     DELETE FROM "{log.schema}"."{log.name}"
                     WHERE schema_name=%s AND relation_name=%s
                     """, (rel.schema, rel.name))
        curs.execute(f"""
                     INSERT INTO "{log.schema}"."{log.name}"
                     (schema_name, relation_name, loaded)
                     VALUES
                     (%s, %s, %s)
                     """, (rel.schema, rel.name, last_upload))


def main():
    log = Relation("target", "public", "etl_runs")
    log_columns = [
        Column("schema_name", "TEXT"),
        Column("relation_name", "TEXT"),
        Column("loaded", "TIMESTAMPTZ")
    ]

    source_address = Relation("source", "public", "address")
    target_address = Relation("target", "public", "address")

    address_columns = get_columns(source_address)

    source_company = Relation("source", "public", "company")
    target_company = Relation("target", "public", "company")

    company_columns = get_columns(source_address)

    make_sure_table_exists(address_columns, target_address, "id")
    make_sure_table_exists(company_columns, target_company, "company_Id")
    make_sure_table_exists(log_columns, log)

    last_upload = get_last_uploaded_time(target_address, log)
    now = datetime.now()
    copy(source_address, target_address, "id", "created_at", last_upload)
    log_upload(target_address, log, now)

    last_upload = get_last_uploaded_time(target_company, log)
    now = datetime.now()
    copy(source_company, target_company, "company_id", "created_at", last_upload)
    log_upload(target_address, log, now)


if __name__=='__main__':
    main()
