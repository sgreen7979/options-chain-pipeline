#!/usr/bin/env python3

from pathlib import Path
from typing import Optional

from daily.sql.mssql.create_server_connection import create_server_connection


def prepare_bulk_insert_query(
    fpath: str,
    table: str,
    firstrow: int = 2,
    fieldterminator: str = ',',
    rowterminator: str = '0x0A',
    lastrow: Optional[int] = None,
    maxerrors: int = int(1e9),
    fire_triggers: bool = False,
):
    sql_template = """
    BULK INSERT {table}
    FROM '{absolute_fpath}'
    WITH (
        FIRSTROW={firstrow},
        LASTROW={lastrow},
        FIELDTERMINATOR='{fieldterminator}',
        ROWTERMINATOR ='{rowterminator}',
        MAXERRORS = {maxerrors},
        FIRE_TRIGGERS
    )
    """

    def strip_comma_from_last_line(new_sql):
        for idx, i in enumerate(new_sql):
            if i.strip() == ")":
                new_sql[idx - 1] = new_sql[idx - 1].replace(',', '')
        return new_sql

    absolute_fpath = Path(fpath).absolute()
    sql = sql_template.format(
        absolute_fpath=absolute_fpath,
        table=table,
        firstrow=firstrow,
        lastrow=lastrow,
        fieldterminator=fieldterminator,
        rowterminator=rowterminator,
        maxerrors=maxerrors,
    )
    if lastrow is None:
        new_sql = []
        for line in sql.splitlines():
            if line.strip() != "LASTROW=None,":
                new_sql.append(line)
        sql = '\n'.join(new_sql)
    if not fire_triggers:
        new_sql = []
        for line in sql.splitlines():
            if line.strip() != "FIRE_TRIGGERS":
                new_sql.append(line)
        new_sql = strip_comma_from_last_line(new_sql)
        sql = '\n'.join(new_sql)

    return sql


def bulk_insert_pyodbc(
    fpath: str,
    table: str,
    firstrow: int = 2,
    fieldterminator: str = ',',
    rowterminator: str = '0x0A',
    lastrow: Optional[int] = None,
    maxerrors: int = int(1e9),
    verbose: bool = False,
    fire_triggers: bool = False,
    no_insert: bool = False,
):
    """
    SQL Server bulk insert syntax
    ------------------------------

        BULK INSERT
       { database_name.schema_name.table_or_view_name | schema_name.table_or_view_name | table_or_view_name }
          FROM 'data_file'
         [ WITH
        (
       [ [ , ] BATCHSIZE = batch_size ]\n
       [ [ , ] CHECK_CONSTRAINTS ]\n
       [ [ , ] CODEPAGE = { 'ACP' | 'OEM' | 'RAW' | 'code_page' } ]\n
       [ [ , ] DATAFILETYPE =
          { 'char' | 'native' | 'widechar' | 'widenative' } ]\n
       [ [ , ] DATA_SOURCE = 'data_source_name' ]\n
       [ [ , ] ERRORFILE = 'file_name' ]\n
       [ [ , ] ERRORFILE_DATA_SOURCE = 'errorfile_data_source_name' ]\n
       [ [ , ] FIRSTROW = first_row ]\n
       [ [ , ] FIRE_TRIGGERS ]\n
       [ [ , ] FORMATFILE_DATA_SOURCE = 'data_source_name' ]\n
       [ [ , ] KEEPIDENTITY ]\n
       [ [ , ] KEEPNULLS ]\n
       [ [ , ] KILOBYTES_PER_BATCH = kilobytes_per_batch ]\n
       [ [ , ] LASTROW = last_row ]\n
       [ [ , ] MAXERRORS = max_errors ]\n
       [ [ , ] ORDER ( { column [ ASC | DESC ] } [ ,...n ] ) ]\n
       [ [ , ] ROWS_PER_BATCH = rows_per_batch ]\n
       [ [ , ] ROWTERMINATOR = 'row_terminator' ]\n
       [ [ , ] TABLOCK ]\n

       -- input file format options\n
       [ [ , ] FORMAT = 'CSV' ]\n
       [ [ , ] FIELDQUOTE = 'quote_characters']\n
       [ [ , ] FORMATFILE = 'format_file_path' ]\n
       [ [ , ] FIELDTERMINATOR = 'field_terminator' ]\n
       [ [ , ] ROWTERMINATOR = 'row_terminator' ]\n
        )]
    """

    sql = prepare_bulk_insert_query(
        fpath=fpath,
        table=table,
        firstrow=firstrow,
        fieldterminator=fieldterminator,
        rowterminator=rowterminator,
        lastrow=lastrow,
        maxerrors=maxerrors,
        fire_triggers=fire_triggers,
    )
    if verbose:
        print(sql)
    if not no_insert:
        with create_server_connection() as cnxn:
            cnxn.execute(sql)


bulk_insert = bulk_insert_pyodbc


# def main():
#     table = 'options1'
#     import time
#     start = time.perf_counter()
#     bulk_insert(table, TEST_FILE)
#     end = time.perf_counter()
#     elapsed = end - start
#     if elapsed >= 1:
#         print('took {}s'.format(elapsed))
#     else:
#         print('took {}ms'.format(elapsed*1000))

# if __name__ == "__main__":
#     main()

# ********************************************************************************************
# ********************************************************************************************
# ********************************************************************************************

# engine_coha35q = create_engine(
#     'mssql+pyodbc://grn:nyknicks999@localhost:1433/stock_database_2?driver=ODBC Driver 17 for SQL Server'
# )
# Session: sessionmaker  = sessionmaker(engine_coha35q)
# with Session.begin() as session:
#     session.add()

# DIR = './_chains'
# TEST_FILE = 'option_chain.BIGTEST.csv'
# TEST_DIR = '{}/{}'.format(DIR, TEST_FILE)
