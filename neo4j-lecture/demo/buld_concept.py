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

df = pd.read_csv("./stock_concept_prep.csv",  dtype=str, keep_default_na=False)
df = df.rename(columns={"c_name": "concept"})

df_concept = DataFrame(df, columns=["concept"])
df_concept = df_concept.drop_duplicates(["concept"])
df_concept.to_csv("./concept.csv", index=False, columns=["concept"])


df_stock_concept = DataFrame(df, columns=["code", "concept"])
df_stock_concept.to_csv("./stock_concept.csv", index=False, columns=["code", "concept"])





