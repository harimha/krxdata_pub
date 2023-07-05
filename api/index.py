import pandas as pd
from typing import overload
from krxdata.preprocessing import dfhandling
from krxdata.rawdata.index \
    import 개별지수시세추이, 주가지수코드검색, 전체지수기본정보, 지수구성종목, PER_PBR_배당수익률


def get_index_info():
    obj = 전체지수기본정보()
    index_class_list = ["KRX", "KOSPI", "KOSDAQ", "테마"]
    df = pd.DataFrame(columns=["지수명", "영문지수명", "기준일", "발표일", "기준지수",
                               "산출주기", "산출시간", "구성종목수", "full_code",
                               "short_code","index_class"])
    for index_class in index_class_list:
        df_data = obj.get_data(index_class)
        df_data["index_class"] = index_class
        df = pd.concat([df,df_data],axis=0)
    col_datetime = ["기준일", "발표일"]
    col_float = ["기준지수"]
    col_int = ["구성종목수"]
    df = dfhandling.remove_comma(df, df.columns)
    df = dfhandling.change_type(df,
                                datetime=col_datetime,
                                float=col_float,
                                int=col_int)
    df.drop(["full_code", "short_code"], axis=1, inplace=True)
    df.index = range(len(df))

    return df

def get_index_components(index_name, search_date):
    obj = 지수구성종목()
    df = obj.get_data(index_name, search_date)
    df = df[["종목코드", "종목명", "종가", "상장시가총액"]]
    int_col = ["종가", "상장시가총액"]
    df = dfhandling.remove_comma(df, df.columns)
    df = dfhandling.change_type(df, int=int_col)
    df["조회일"] = search_date
    df["지수명"] = index_name

    return df

@overload
def get_index_PER(search_date, index_class):
    ...

@overload
def get_index_PER(index_name, sdate, edate):
    ...

def get_index_PER(*args):
    """
    :param index_class = ["KRX" | "KOSPI" | "KOSDAQ" | "테마"]
    """
    obj = PER_PBR_배당수익률()
    if len(args) == 2:
        search_date, index_class = args
        df = obj.get_data(search_date, index_class)
        df = df[["지수명","종가","PER","선행PER"]]
        col_float = ["종가","PER","선행PER"]
        df = dfhandling.remove_hyphen(df)
        df = dfhandling.remove_comma(df, df.columns)
        df = dfhandling.change_type(df, float=col_float)
    else:
        index_name, sdate, edate = args
        df = obj.get_data(index_name, sdate, edate)
        df["지수명"] = index_name
        df = df[["일자", "종가", "PER", "선행PER", "지수명"]]
        col_datetime = ["일자"]
        col_float = ["종가", "PER", "선행PER"]
        df = dfhandling.remove_hyphen(df)
        df = dfhandling.remove_comma(df, df.columns)
        df = dfhandling.change_type(df, datetime=col_datetime, float=col_float)

    return df

@overload
def get_index_PBR(search_date, index_class):
    ...

@overload
def get_index_PBR(index_name, sdate, edate):
    ...

def get_index_PBR(*args):
    """
    :param index_class = ["KRX" | "KOSPI" | "KOSDAQ" | "테마"]
    """
    obj = PER_PBR_배당수익률()
    if len(args) == 2:
        search_date, index_class = args
        df = obj.get_data(search_date, obj.index_class[index_class])
        df = df[["지수명","종가","PBR"]]
        col_float = ["종가","PBR"]
        df = dfhandling.remove_hyphen(df)
        df = dfhandling.remove_comma(df, df.columns)
        df = dfhandling.change_type(df, float=col_float)
    else:
        index_name, sdate, edate = args
        df = obj.get_data(index_name, sdate, edate)
        df["지수명"] = index_name
        df = df[["일자", "종가", "PBR", "지수명"]]
        col_datetime = ["일자"]
        col_float = ["종가", "PBR"]
        df = dfhandling.remove_hyphen(df)
        df = dfhandling.remove_comma(df, df.columns)
        df = dfhandling.change_type(df, datetime=col_datetime, float=col_float)

    return df

@overload
def get_index_div_rate(search_date, index_class):
    ...

@overload
def get_index_div_rate(index_name, sdate, edate):
    ...

def get_index_div_rate(*args):
    """
    :param index_class = ["KRX" | "KOSPI" | "KOSDAQ" | "테마"]
    """
    obj = PER_PBR_배당수익률()
    if len(args) == 2:
        search_date, index_class = args
        df = obj.get_data(search_date, obj.index_class[index_class])
        df = df[["지수명","종가","배당수익률"]]
        col_float = ["종가","배당수익률"]
        df = dfhandling.remove_hyphen(df)
        df = dfhandling.remove_comma(df, df.columns)
        df = dfhandling.change_type(df, float=col_float)
    else:
        index_name, sdate, edate = args
        df = obj.get_data(index_name, sdate, edate)
        df["지수명"] = index_name
        df = df[["일자", "종가", "배당수익률", "지수명"]]
        col_datetime = ["일자"]
        col_float = ["종가", "배당수익률"]
        df = dfhandling.remove_hyphen(df)
        df = dfhandling.remove_comma(df, df.columns)
        df = dfhandling.change_type(df, datetime=col_datetime, float=col_float)

    return df

def get_index_ohlcv(index_name, sdate, edate) -> pd.DataFrame:
    '''
    :param index_name: ex)코스피200
    :param sdate: 조회시작일
    :param edate: 조회 종료일
    '''
    obj = 개별지수시세추이()
    df = obj.get_data(index_name, sdate, edate)
    df.drop(["대비","등락률"], axis=1, inplace=True)
    col_datetime = df.columns[:1]
    col_float = df.columns[1:5]
    col_int = df.columns[5:]
    df = dfhandling.remove_hyphen(df, zero=True)
    df = dfhandling.remove_comma(df,df.columns)
    df = dfhandling.change_type(df,
                                datetime=col_datetime,
                                float=col_float,
                                int=col_int)
    df["index_name"] = index_name

    return df

def get_index_code() -> pd.DataFrame:
    '''지수 코드 관련 데이터'''
    obj = 주가지수코드검색()
    df = obj.get_data()

    return df
