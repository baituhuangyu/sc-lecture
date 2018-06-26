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

df = pd.read_csv("./stock_industry_prep.csv",  dtype=str, keep_default_na=False)
df = df.rename(columns={"c_name": "industry"})


df_industry = DataFrame(df, columns=["industry"])
df_industry = df_industry.drop_duplicates(["industry"])
df_industry.to_csv("./industry.csv", index=False, columns=["industry"])

df_stock_industry = DataFrame(df, columns=["code", "industry"])
df_stock_industry.to_csv("./stock_industry.csv", index=False, columns=["code", "industry"])








