#!/usr/bin/env python3
# encoding: utf-8
from __future__ import print_function
import tushare as ts
import sys
reload(sys)
sys.setdefaultencoding('utf8')

df_industry = ts.get_industry_classified()
df_industry.to_csv("./stock_industry_prep.csv", index=False)
print(df_industry)


df_concept = ts.get_concept_classified()
print(df_concept)
df_concept.to_csv("./stock_concept_prep.csv", index=False)


