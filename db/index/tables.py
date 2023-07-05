from krxdata.api import index
from krxdata.db.schemas import SecurityMarket
from db.index.mixin import CrossSectional, TimeSeries


class IndexCode(SecurityMarket, CrossSectional):
    def __init__(self):
        super().__init__()
        self.table_name = "index_code"
        self.columns = ['full_code', 'short_code', 'index_name', 'market_code', 'market_name']
        self.types = ['varchar(3)', 'varchar(6)', 'varchar(50)', 'varchar(6)', 'varchar(10)']
        self.pkey = ["full_code","short_code"]
        self.get_data = index.get_index_code
        self.create_table()
        self.table_obj = self.get_table_obj()


class IndexInfo(SecurityMarket, CrossSectional):
    def __init__(self):
        super().__init__()
        self.table_name = "index_info"
        self.columns = ['지수명', '영문지수명', '기준일', '발표일', '기준지수', '산출주기', '산출시간', '구성종목수', 'index_class']
        self.types = ['varchar(30)', 'varchar(50)', 'datetime', 'datetime', 'float', 'varchar(10)', 'varchar(30)', 'int', 'varchar(10)']
        self.pkey = ["지수명"]
        self.get_data = index.get_index_info
        self.create_table()
        self.table_obj = self.get_table_obj()


class IndexComponents(SecurityMarket, CrossSectional):
    def __init__(self):
        super().__init__()
        self.table_name = "index_components"
        self.columns = ['종목코드', '종목명', '종가', '상장시가총액', '조회일', '지수명']
        self.types = ['varchar(10)', 'varchar(30)', 'float', 'bigint', 'datetime', 'varchar(30)']
        self.pkey = ['종목코드', '조회일']
        self.get_data = index.get_index_components
        self.create_table()
        self.table_obj = self.get_table_obj()


class IndexOHLCV(SecurityMarket, TimeSeries):
    def __init__(self):
        super().__init__()
        self.table_name = "index_ohlcv"
        self.columns = ['일자', '종가', '시가', '고가', '저가', '거래량', '거래대금', '상장시가총액','index_name']
        self.types = ['datetime', 'float', 'float', 'float', 'float', 'bigint', 'bigint', 'bigint','varchar(50)']
        self.pkey = ['일자','index_name']
        self.get_data = index.get_index_ohlcv
        self.create_table()
        self.table_obj = self.get_table_obj()


class IndexPER(SecurityMarket, TimeSeries):
    def __init__(self):
        super().__init__()
        self.table_name = "index_per"
        self.columns = ['일자', '종가', 'PER', '선행PER', '지수명']
        self.types = ['datetime', 'float', 'float', 'float', 'varchar(50)']
        self.pkey = ['일자', '지수명']
        self.get_data = index.get_index_PER
        self.create_table()
        self.table_obj = self.get_table_obj()


class IndexPBR(SecurityMarket, TimeSeries):
    def __init__(self):
        super().__init__()
        self.table_name = "index_pbr"
        self.columns = ['일자', '종가', '시가', '고가', '저가', '거래량', '거래대금', '상장시가총액', 'index_name']
        self.types = ['datetime', 'float', 'float', 'float', 'float', 'bigint', 'bigint', 'bigint', 'varchar(50)']
        self.pkey = ['일자', 'index_name']
        self.get_data = index.get_index_ohlcv
        self.create_table()
        self.table_obj = self.get_table_obj()


class IndexDivRate(SecurityMarket, TimeSeries):
    def __init__(self):
        super().__init__()
        self.table_name = "index_divrate"
        self.columns = ['일자', '종가', '시가', '고가', '저가', '거래량', '거래대금', '상장시가총액', 'index_name']
        self.types = ['datetime', 'float', 'float', 'float', 'float', 'bigint', 'bigint', 'bigint', 'varchar(50)']
        self.pkey = ['일자', 'index_name']
        self.get_data = index.get_index_ohlcv
        self.create_table()
        self.table_obj = self.get_table_obj()


