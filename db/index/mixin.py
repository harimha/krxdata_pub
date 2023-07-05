import time
import pandas as pd
from typing import overload
from datetime import timedelta
from sqlalchemy import update, select
from krxdata.db.base import Table
from krxdata.common.utils import date_format, get_last_buisiness_day
from db.utils import split_period, is_within_one_year, hasNull


class TableOperation(Table):
    def __init__(self):
        super().__init__()

    def create_table(self):
        super().create_table(self.schema_name, self.table_name)
        if self._isColumnExists(self.schema_name, self.table_name, "tmp"):
            col_type = dict(zip(self.columns, self.types))
            for col_name, col_type in col_type.items():
                self.add_column(col_name, col_type)
            self.drop_column("tmp")  # 임시 칼럼 삭제
            try:
                self.drop_primarykey()
            except:
                pass
            self.add_primarykey(self.pkey)
        else: pass

    def drop_table(self):
        super().drop_table(self.schema_name, self.table_name)

    def add_column(self, col_name, col_type):
        super().add_column(self.schema_name, self.table_name, col_name, col_type)

    def drop_column(self, col_name):
        super().drop_column(self.schema_name, self.table_name, col_name)

    def alter_constraint(self, constraint, col_obj):
        '''
        UNIQUE, NOT NULL, CHECK, DEFAULT, AUTO_INCREMENT, ENUM 등 사용
        ex) alter_constraint(table_name, "not null", 'full_code')
        '''
        super().alter_constraint(self.schema_name, self.table_name, constraint, col_obj)

    def drop_primarykey(self):
        super().drop_primarykey(self.schema_name, self.table_name)

    def add_primarykey(self, columns: str or list):
        '''
        ex) add_primarykey(schema_name, table_name, ["col1","col2"])
        add_primarykey(schema_name, table_name, "col1")
        :param columns: str or list_like type
        '''
        super().add_primarykey(self.schema_name, self.table_name, columns)

    def insert_data(self, values: pd.Series):
        super().insert_data(self.table_obj, values)

    def get_table_obj(self):
        table_obj = super().get_table_obj(self.schema_name, self.table_name)

        return table_obj

    def get_column_obj(self, col_name):
        col_obj = super().get_column_obj(self.schema_name, self.table_name, col_name)

        return col_obj

    def df_to_db(self, df):
        for i in range(len(df)):
            data = df.iloc[i]
            if hasNull(data):
                data = data[data.notnull()]
            try:
                self.insert_data(data)
            except:
                continue


class CrossSectional(TableOperation):

    def read_db(self, columns):
        if isinstance(columns, list):
            columns = tuple(columns)
        tbo = self.get_table_obj()
        stmt = select(tbo.c[columns])
        df = pd.read_sql(stmt, self.engine)

        return df

    @overload
    def store_data(self, index_name, search_date):
        ...
    @overload
    def store_data(self):
        ...

    def store_data(self, *args):
        if len(args) == 2:
            index_name, search_date = args
            self.create_table()
            df = self.get_data(index_name, search_date)
            self.df_to_db(df)
        else:
            self.create_table()
            df = self.get_data()
            self.df_to_db(df)


class TimeSeries(TableOperation):
    def __init__(self):
        super().__init__()

    def read_db(self, index_name, columns, sdate=None, edate=None):
        tbo = self.get_table_obj()
        if isinstance(columns, list):
            columns = tuple(columns)
        if (sdate == None) & (edate == None) :
            stmt = select(tbo.c[columns]).where(tbo.c["index_name"]==index_name)
        else:
            stmt = select(tbo.c[columns]).where(tbo.c["index_name"]==index_name,
                                                tbo.c["일자"].between(sdate,edate))
        df = pd.read_sql(stmt,self.engine)

        return df

    def store_data_period(self, index_name, sdate, edate, sleep_time=1.5):
        sdate, edate = date_format(sdate, edate)

        if is_within_one_year(sdate, edate):
            df = self.get_data(index_name, sdate, edate)
            self.df_to_db(df)
            print(f"{index_name} is stored {format(sdate,'%Y-%m-%d')}~{format(edate,'%Y-%m-%d')}")
        else:
            date_list = split_period(sdate, edate, interval=365)
            for sdate, edate in date_list:
                time.sleep(sleep_time)
                try:
                    df = self.get_data(index_name, sdate, edate)
                    self.df_to_db(df)
                    print(f"{index_name} is stored {format(sdate, '%Y-%m-%d')}~{format(edate, '%Y-%m-%d')}")
                except:
                    print(f"{index_name} is empty {format(sdate, '%Y-%m-%d')}~{format(edate, '%Y-%m-%d')}")

    def modify_data(self, index_name, date, values):
        '''해당 날짜의 데이터 값 수정'''
        tbo = self.table_obj
        stmt = update(tbo).where((tbo.c["일자"] == date) &
                                 (tbo.c["index_name"] == index_name)).values(values)
        self.commit_statement(stmt)

    def modify_df_to_db(self, index_name, df):
        '''dataframe 데이터를 입력받아 db에 업데이트'''
        for i in range(len(df)):
            date = df["일자"].iloc[i]
            values = df.iloc[i][1:8]
            self.modify_data(index_name, date, values)

    def update_data_period(self, index_name, sdate, edate, sleep_time=1.5):
        if is_within_one_year(sdate, edate):
            df = self.get_data(index_name, sdate, edate)
            self.modify_df_to_db(index_name, df)
            print(f"{sdate}~{edate}")
        else:
            date_list = split_period(sdate, edate, 365)
            for sdate, edate in date_list:
                df = self.get_data(index_name, sdate, edate)
                self.modify_df_to_db(index_name, df)
                print(f"{sdate}~{edate}")
                time.sleep(sleep_time)

    def update_db_uptodate(self, index_name, sleep_time=1.5):
        edate = get_last_buisiness_day()
        sdate = self.read_db(index_name, "일자").max()[0]
        if edate == sdate:
            print(f"{index_name} is already up to date")
        else:
            self.store_data_period(index_name, sdate+timedelta(1), edate, sleep_time)




