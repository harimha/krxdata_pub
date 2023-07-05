import os
import pandas as pd
from datetime import timedelta
from sqlalchemy import MetaData, create_engine, select
from krxdata.common.utils import get_last_buisiness_day, date_format


def read_db(schema_name, table_name, columns:tuple):
    table_obj = get_table_obj(schema_name, table_name)
    engine = create_engine(os.getenv("MYSQL_URL"))
    stmt = select(table_obj.c[columns])
    df = pd.read_sql(stmt,engine)

    return df

def get_table_obj(schema_name, table_name):
    engine = create_engine(os.getenv("MYSQL_URL"))
    meta = MetaData(schema_name)
    meta.reflect(engine)
    table = meta.tables[f"{schema_name}.{table_name}"]

    return table

def get_columns(tb_obj):
    columns = tb_obj.c.values()

    return columns

def is_db_uptodate(schema_name, table_name, code_name):
    last_buisiness_day = get_last_buisiness_day()
    engine = create_engine(os.getenv("MYSQL_URL"))
    tb = get_table_obj(schema_name, table_name)
    stmt = select(tb.c["일자"]).where(tb.c["일자"]==last_buisiness_day, tb.c["code_name"]==code_name)
    df = pd.read_sql(stmt, engine)
    if len(df) > 0:
        return True
    else:
        return False

def split_period(sdate, edate, interval:int):
    sdate, edate = date_format(sdate, edate)
    diff = edate-sdate
    date = []
    if diff < timedelta(interval):
        date.append((sdate,edate))
    else:
        interval = timedelta(interval)
        mdate = sdate + interval
        while mdate < edate:
            date.append((sdate,mdate))
            sdate = mdate+ timedelta(1)
            mdate += interval
        date.append((sdate,edate))

        return date

def is_within_one_year(sdate, edate):
    '''1년 이내인지 체크'''
    sdate, edate = date_format(sdate, edate)
    period = edate - sdate
    if period < timedelta(365):
        return True
    else:
        return False

def hasNull(data:pd.Series):
    if sum(data.isnull()):
        return True
    else:
        return False








