import os
import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv()

DB_URL= os.getenv("DATABASE_URL")

@pytest.fixture(scope="session")
def conn():
    if not DB_URL:
        pytest.fail("DATABASE_URL não definida. Defina no .env ou export na sessão.")
    with psycopg.connect(DB_URL) as c:
        yield c

def fetch_one(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None

def fetch_rows(conn, sql, limit=100):
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchmany(limit)
    return rows

def test_silver_exists(conn):
    sql = """
    select count(*)
    from information_schema.views
    where table_schema = 'silver'
        and table_name in ('stg_macro_daily', 'stg_quotes_daily');
    """
    cnt = fetch_one(conn, sql)
    assert cnt == 2, "Views do Silver não encontradas (stg_macro_daily / stg_quotes_daily)."

def test_macro_keys_not_null(conn):
    sql = """
    select count(*) as null_keys
    from silver.stg_macro_daily
    where source is null or source = '' or date is null;
    """
    cnt = fetch_one(conn, sql)
    assert cnt == 0, f"Existem {cnt} registros com chave nula/vazia em stg_macro_daily."

def test_quotes_no_duplicates(conn):
    sql = """
    select count(*) from (
      select source, symbol, date, count(*) as n
      from silver.stg_quotes_daily
      group by 1,2,3
      having count(*) > 1
    ) t;
    """
    cnt = fetch_one(conn, sql)
    assert cnt == 0, f"Há {cnt} grupos duplicados em stg_quotes_daily (source,symbol,date)."


def test_source_symbol_normalization(conn):
    sql_bad_source_macro = """
    select count(*) from silver.stg_macro_daily
    where source <> lower(trim(source));
    """
    sql_bad_source_quotes = """
    select count(*) from silver.stg_quotes_daily
    where source <> lower(trim(source));
    """
    sql_bad_symbol = """
    select count(*) from silver.stg_quotes_daily
    where symbol <> upper(trim(symbol));
    """

    bad_macro = fetch_one(conn, sql_bad_source_macro)
    bad_quotes = fetch_one(conn, sql_bad_source_quotes)
    bad_symbol = fetch_one(conn, sql_bad_symbol)

    assert bad_macro == 0, f"Fonte não normalizada em macro: {bad_macro} linhas."
    assert bad_quotes == 0, f"Fonte não normalizada em quotes: {bad_quotes} linhas."
    assert bad_symbol == 0, f"Symbol não normalizado (MAIÚSCULO/trim): {bad_symbol} linhas."


def test_quotes_value_rules(conn):
    sql = """
    select count(*) from silver.stg_quotes_daily
    where (high is not null and low is not null and high < low)
       or (open  is not null and open  < 0)
       or (high  is not null and high  < 0)
       or (low   is not null and low   < 0)
       or (close is not null and close < 0);
    """
    cnt = fetch_one(conn, sql)
    assert cnt == 0, f"Valores suspeitos em quotes: {cnt} linhas (negativos ou high<low)."