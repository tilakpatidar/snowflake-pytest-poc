import os

import numpy
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal
from pytest_bdd import then, when, parsers, given
from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy.snowdialect import ischema_names as snowflake_to_sql_alchemy_types
from sqlalchemy import create_engine, Column, Table, MetaData

snowflake_user = os.environ['SNOWFLAKE_USER']
snowflake_password = os.environ['SNOWFLAKE_PASSWORD']
snowflake_account = os.environ['SNOWFLAKE_ACCOUNT']


@pytest.fixture(scope="function", autouse=True)
def snowflake_sql_conn():
    engine, connection = _create_snowflake_sql_alchemy_engine()
    yield engine
    print(f"Closing snowflake connection")
    connection.close()
    engine.dispose()


def _create_snowflake_sql_alchemy_engine():
    print(f"Creating snowflake connection for account: {snowflake_account} user: {snowflake_user}")
    engine = create_engine(URL(
        account=snowflake_account,
        user=snowflake_user,
        password=snowflake_password,
    ))
    connection = engine.connect()
    return engine, connection


def assert_frame_equal_with_sort(results, expected, key_columns, dtype_check=True):
    results_sorted = results.sort_values(
        by=key_columns).reset_index(drop=True).sort_index(axis=1)
    expected_sorted = expected.sort_values(
        by=key_columns).reset_index(drop=True).sort_index(axis=1)
    assert_frame_equal(results_sorted, expected_sorted,
                       check_index_type=False, check_dtype=dtype_check)


def snowflake_type_to_dtype(tname):
    return numpy.dtype(snowflake_type_to_sqltype(tname).python_type)


def snowflake_type_to_sqltype(tname):
    return snowflake_to_sql_alchemy_types.get(tname.strip())()


def process_cells(cells):
    for cell in cells:
        cell = cell.strip()
        if cell[0] == '"' and cell[-1] == '"':
            yield cell[1:-1]
        elif cell == "<null>":
            yield None
        else:
            yield cell


def table_to_df(table):
    heading = table.split("\n")[0]
    col_names_with_types = list(map(lambda header: header.strip(), heading.split("|")[1:-1]))
    col_name_dtype_pairs = []
    col_name_sqltype_pairs = []
    for col_name_with_type in col_names_with_types:
        if len(col_name_with_type.split(':')) != 2:
            raise ValueError(
                f"You must specify name AND data type for columns like this 'my_field:string' at {col_name_with_type}")
        col_name, col_type = col_name_with_type.split(':')
        col_name_dtype_pairs.append((col_name.strip(), snowflake_type_to_dtype(col_type.strip())))
        col_name_sqltype_pairs.append((col_name.strip(), snowflake_type_to_sqltype(col_type.strip())))
    table_body = table.split("\n")[1:]
    rows = [list(process_cells(row.split("|")[1:-1])) for row in table_body]

    df = pd.DataFrame(
        rows,
        columns=[col_name_type_pair[0] for col_name_type_pair in col_name_dtype_pairs],
    )

    df = df.astype(
        dict(col_name_dtype_pairs)
    )

    return df, dict(col_name_dtype_pairs), col_name_sqltype_pairs


@when(parsers.re(r'a table called "(?P<table_name>.+)" has\s+(?P<table>[\s\S]+)'))
def table_create_fixture(snowflake_sql_conn, table_name, table):
    df, col_name_dtype_pairs, col_name_sqltype_pairs = table_to_df(table)
    db_name, schema_name, tb_name = table_name.split(".")

    cols = [
        Column(col_name, col_type)
        for col_name, col_type in col_name_sqltype_pairs
    ]
    snowflake_sql_conn.execute(f"USE DATABASE \"{db_name}\"")

    metadata = MetaData(bind=snowflake_sql_conn)
    temp_table = Table(tb_name,
                       metadata,
                       *cols,
                       schema=schema_name,
                       prefixes=['TEMPORARY'],
                       )

    temp_table.create(bind=snowflake_sql_conn)
    df.to_sql(con=snowflake_sql_conn,
              schema=schema_name,
              name=tb_name,
              if_exists="append",
              method="multi",
              index=False)


@then(parsers.re(r'a sql script "(?P<script_path>.+)" runs and the result is\n(?P<table>[\s\S]+)'))
def assert_table_contains(snowflake_sql_conn, script_path, table):
    sql = open(script_path, "r").read()
    actual_df = pd.read_sql(sql, snowflake_sql_conn)
    expected_df, _, _ = table_to_df(table)

    print("\n\n\nEXPECTED schema")
    print(expected_df.dtypes)
    print("\n\n\nACTUAL schema")
    print(actual_df.dtypes)

    assert_frame_equal(actual_df, expected_df)


@given('a snowflake connection')
def t():
    pass
