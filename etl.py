import contextlib
from datetime import date
from functools import namedtuple
import psycopg2
from typing import Dict, List, Union


def get_connect_arguments() -> Dict[str, str]:
    dsn = dict()
    try:
        import config
    except ImportError:
        return dsn

    if hasattr(config, "db") and isinstance(config.db, dict):
        dsn.update(config.db)
    return dsn


@contextlib.contextmanager
def connect(db: str):
    conn, cursor = None, None
    try:
        dsn = get_connect_arguments()
        dsn["dbname"] = db
        conn = psycopg2.connect(**dsn)
        cursor = conn.cursor()
        yield conn, cursor
    finally:
        if conn:
            conn.commit()
        if cursor:
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
            [f'"{col.name}" {col.type}{" UNIQUE" if key==col.name else ""}' for col in columns]
        )
        curs.execute(f"""
                     CREATE TABLE IF NOT EXISTS "{rel.schema}"."{rel.name}" (
                         {cols_ddl}
                     );
                     """)


def get_last_uploaded_time(rel: Relation, log: Relation) -> Union[date, None]:
    with connect(log.db) as (_, curs):
        curs.execute(f"""
                     SELECT loaded
                     FROM "{log.schema}"."{log.name}"
                     WHERE schema_name=%s AND relation_name=%s;
                     """, (rel.schema, rel.name))
        row = curs.fetchone()
        if row:
            return row[0]


def copy(src: Relation, tgt: Relation, key_field: str, tstz_field: str, last_upload: date):
    with connect(src.db) as (_, curs):
        # Since the starting date for the extraction can be null, daterange is used as it treats NULL as "no limit"
        curs.execute(f"""
                     SELECT *
                     FROM "{src.schema}"."{src.name}"
                     WHERE "{tstz_field}" <@ DATERANGE(%s::DATE, NULL, '[)');
                     """, (last_upload, ))

        columns = '", "'.join(desc[0] for desc in curs.description)
        placeholders = ', '.join(['%s'] * len(curs.description))

        insert_query = f"""
        INSERT INTO "{tgt.schema}"."{tgt.name}"
        ("{columns}")
        VALUES ({placeholders})
        ON CONFLICT ("{key_field}") DO NOTHING;
        """
        with connect(tgt.db) as (_, tgt_curs):
            tgt_curs.executemany(insert_query, curs.fetchall())


def log_upload(rel: Relation, log: Relation, last_upload: date):
    with connect(log.db) as (_, curs):
        curs.execute(f"""
                     DELETE FROM "{log.schema}"."{log.name}"
                     WHERE schema_name=%s AND relation_name=%s;
                     """, (rel.schema, rel.name))
        curs.execute(f"""
                     INSERT INTO "{log.schema}"."{log.name}"
                     (schema_name, relation_name, loaded)
                     VALUES
                     (%s, %s, %s);
                     """, (rel.schema, rel.name, last_upload))


def main():
    log = Relation("target", "public", "etl_runs")
    log_columns = [
        Column("schema_name", "TEXT"),
        Column("relation_name", "TEXT"),
        Column("loaded", "TIMESTAMPTZ")
    ]

    print("Getting source data schema")

    source_address = Relation("source", "public", "address")
    target_address = Relation("target", "public", "address")

    address_columns = get_columns(source_address)

    source_company = Relation("source", "public", "company")
    target_company = Relation("target", "public", "company")

    company_columns = get_columns(source_company)

    print("Ensuring schema at destination")

    make_sure_table_exists(target_address, address_columns, "id")
    make_sure_table_exists(target_company, company_columns, "company_id")
    make_sure_table_exists(log, log_columns, None)
    
    print("Copying data")

    last_upload = get_last_uploaded_time(target_address, log)
    print(f"Getting address data since {last_upload}")
    today = date.today()
    copy(source_address, target_address, "id", "created_at", last_upload)
    log_upload(target_address, log, today)

    last_upload = get_last_uploaded_time(target_company, log)
    print(f"Getting company data since {last_upload}")
    today = date.today()
    copy(source_company, target_company, "company_id", "created_at", last_upload)
    log_upload(target_company, log, today)

    print("Done")


if __name__=='__main__':
    main()
