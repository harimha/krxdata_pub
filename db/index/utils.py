from krxdata.db.utils import read_db


def code_to_name(full_code, short_code):
    schema_name = "security_market"
    table_name = "index_code"
    df = read_db(schema_name,table_name,("full_code", "short_code", "index_name"))
    cond1 = df["full_code"] == full_code
    cond2 = df["short_code"] == short_code
    index_name = df["index_name"][cond1 & cond2].iloc[0]

    return index_name

def name_to_code(index_name):
    schema_name = "security_market"
    table_name = "index_code"
    df = read_db(schema_name,table_name,("full_code", "short_code", "index_name"))
    cond = df["index_name"] == index_name
    full_code, short_code = tuple(df[["full_code", "short_code"]][cond].iloc[0])

    return full_code, short_code

