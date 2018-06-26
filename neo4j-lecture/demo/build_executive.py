#!/usr/bin/env python3
# encoding: utf-8

from __future__ import print_function
import pandas as pd
from pandas import DataFrame
import hashlib
import pdb
import csv
import sys
reload(sys)
sys.setdefaultencoding('utf8')


columns_idex = ["name", "sex", "age", "code", "jobs"]


def md5(str_data):
    md5_value = hashlib.md5()
    md5_value.update(str_data)
    return md5_value.hexdigest()


df = pd.read_csv("./executive_prep.csv", dtype=str, keep_default_na=False)
df["pid"] = df.apply(lambda row: md5(row["name"]+row["sex"]+row["age"]), axis=1)
df = df.drop_duplicates(["pid", "code"])

executive_header = ["pid", "name", "sex", "age"]
executive_stock_header = ["pid", "code", "name", "jobs"]


# df_executive = DataFrame(df, columns=executive_header)
# df_executive = df_executive.drop_duplicates(["pid"])
# df_executive.to_csv("./executive.csv", index=False, columns=executive_header)

df_executive_stock = DataFrame(df, columns=executive_stock_header)
df_executive_stock.to_csv("./executive_stock.csv", index=False, columns=executive_stock_header)


# “executive.csv”, "stock.csv", "concept.csv", "industry.csv", "executive_stock.csv", "stock_industry.csv", "stock_concept.csv"。


# for _, item in df.iterrows():
#     print(item)
#     pdb.set_trace()
#     df_executive = df_executive.append([item["name"], item["sex"], item["age"]])
#
#
#




