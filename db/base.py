import os
import pandas as pd
from krxdata.db import statements as stmts
from sqlalchemy import create_engine, MetaData, inspect, schema, insert
from krxdata.db.configure import Configuration


class MySQL():
    def __init__(self, password=None):
        if password is None :
            try:
                self._url = f"mysql://root:{Configuration().password}@localhost"
            except:
                self._url = os.getenv("MYSQL_URL")
        else:
            self._url = f"mysql://root:{password}@localhost"
        self.engine = create_engine(self._url)
        self.metadata = MetaData()

    def commit_statement(self, stmt):
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()


class Schema(MySQL):
    def __init__(self):
        super().__init__()

    def _isSchemaExists(self, engine, schema_name):
        insp = inspect(engine)
        schema_names = insp.get_schema_names()
        if schema_name in schema_names:
            return True
        else:
            return False

    def create_schema(self, schema_name):
        if self._isSchemaExists(self.engine, schema_name):
            pass
        else:
            stmt = schema.CreateSchema(schema_name)
            self.commit_statement(stmt)

    def drop_schema(self, schema_name):
        if self._isSchemaExists(self.engine,schema_name):
            stmt = schema.DropSchema(schema_name)
            self.commit_statement(stmt)
        else:
            pass


class Table(MySQL):
    def __init__(self):
        super().__init__()

    def _isTableExists(self, schema_name, table_name):
        insp = inspect(self.engine)
        table_names = insp.get_table_names(schema_name)
        if table_name in table_names:
            return True
        else:
            return False

    def _isColumnExists(self, schema_name, table_name, col_name):
        insp = inspect(self.engine)
        columns = insp.get_columns(table_name, schema_name)
        col_names = []
        for col in columns:
            col_names.append(col["name"])
        if col_name in col_names:
            return True
        else:
            return False

    def create_table(self, schema_name, table_name):
        '''임시칼럼 tmp 생성됨'''
        if self._isTableExists(schema_name, table_name):
            pass
        else:
            stmt = stmts.create_table(schema_name, table_name)
            self.commit_statement(stmt)

    def drop_table(self, schema_name, table_name):
        if self._isTableExists(schema_name, table_name):
            stmt = stmts.drop_table(schema_name, table_name)
            self.commit_statement(stmt)
        else: pass

    def add_column(self, schema_name, table_name, col_name, col_type):
        if self._isColumnExists(schema_name, table_name, col_name):
            pass
        else:
            stmt = stmts.add_column(schema_name, table_name, col_name, col_type)
            self.commit_statement(stmt)

    def drop_column(self, schema_name, table_name, col_name):
        if self._isColumnExists(schema_name, table_name, col_name):
            stmt = stmts.drop_column(schema_name, table_name, col_name)
            self.commit_statement(stmt)
        else: pass

    def alter_constraint(self, schema_name, table_name, constraint, col_obj):
        '''
        UNIQUE, NOT NULL, CHECK, DEFAULT, AUTO_INCREMENT, ENUM 등 사용
        ex) alter_constraint(table_name, "not null", 'full_code')
        '''
        stmt = stmts.alter_constraint(schema_name, table_name, constraint, col_obj)
        self.commit_statement(stmt)

    def drop_primarykey(self, schema_name, table_name):
        stmt = stmts.drop_primarykey(schema_name, table_name)
        self.commit_statement(stmt)

    def add_primarykey(self, schema_name, table_name, columns:str or list):
        '''
        ex) add_primarykey(schema_name, table_name, ["col1","col2"])
        add_primarykey(schema_name, table_name, "col1")
        :param columns: str or list_like type
        '''
        stmt = stmts.add_primarykey(schema_name, table_name, columns)
        self.commit_statement(stmt)

    def insert_data(self, table_obj, values:pd.Series):
        stmt = insert(table_obj).values(values)
        self.commit_statement(stmt)

    def get_table_obj(self, schema_name, table_name):
        self.metadata = MetaData(schema_name)
        self.metadata.reflect(self.engine)
        table_obj = self.metadata.tables[f"{schema_name}.{table_name}"]

        return table_obj

    def get_column_obj(self, schema_name, table_name, col_name):
        self.metadata = MetaData(schema_name)
        self.metadata.reflect(self.engine)
        column_obj = self.metadata.tables[f"{schema_name}.{table_name}"].c[col_name]

        return column_obj




