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

df_industry = pd.read_csv("./stock_industry_prep.csv",  dtype=str, keep_default_na=False)
df_name_1 = DataFrame(df_industry, columns=["code", "name"])

df_concept = pd.read_csv("./stock_concept_prep.csv",  dtype=str, keep_default_na=False)
df_name_2 = DataFrame(df_concept, columns=["code", "name"])

fs = pd.concat([df_name_1, df_name_2])

df_stock = DataFrame(fs, columns=["code", "name"])
df_stock = df_stock.drop_duplicates(["code", "name"])
df_stock.to_csv("./stock_2.csv", index=False, columns=["code", "name"])

