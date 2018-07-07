#!/usr/bin/env python3
# encoding: utf-8

from __future__ import print_function
import re
import sys
import os
from bs4 import BeautifulSoup
import json
from collections import OrderedDict
from pandas import Series,DataFrame
import pandas as pd

import csv
reload(sys)
sys.setdefaultencoding('utf8')

all_company_result = {}
executive_prep_json = open("./executive_prep.json", "w+")
# executive_prep_csv = open("./executive_prep.csv", "w+")
file_dir = "/home/huangyu/下载/exe_member/target/"

all_files = map(lambda _: os.path.join(file_dir, _), os.listdir(file_dir))
# all_files = map(lambda _: os.path.join(file_dir, _), ["600510.html"])
all_files = filter(lambda _: _.endswith("html"), all_files)
df = DataFrame()
columns_idex = ["name", "sex", "age", "code", "jobs"]
for a_file in all_files:
    try:
        a_file_name = a_file.split("/")[-1]
        a_code = a_file_name.split(".")[0]
        f = open(a_file, "r")
        h = f.read()
        hs = BeautifulSoup(h)
        # company_name = re.findall("公司名称：</strong><span>(.*?)</span>", str(hs))[0]
        print(a_file_name)
        ml_001 = hs.find(id="ml_001")
        if not ml_001:
            continue

        tbody = ml_001.find("tbody")
        trs = tbody.find_all(attrs={"class": "tc name"})
        mls = map(lambda _: {
            "name": _.find(attrs={"class": "person_info"}).text,
            "jobs": _.find(attrs={"class": "jobs"}).text.replace(u",", u"、"),
            "intro": _.find(attrs={"class": "intro"}).text,
            "code": a_code,
        }, trs)

        map(lambda d: d.update(**{
            "sex": "女" if "女" in d["intro"] else ("男" if "男" in d["intro"] else ""),
            "age": re.findall("\d+", d["intro"])[0] if re.search("\d+", d["intro"]) else "",
        }), mls)

        map(lambda ml: executive_prep_json.write(json.dumps(ml, ensure_ascii=False)+"\n"), mls)
        # 高管姓名、性别、年龄、股票代码、职位
        for item in mls:
            clos = map(lambda k: item.get(k, ""), columns_idex)
            # executive_prep_csv.write(",".join(clos)+"\n")
            df = df.append(DataFrame([clos], columns=columns_idex))
    except Exception as e:
        print(e)
        import pdb
        pdb.set_trace()
        print(ml_001)
        print(tbody)

    finally:
        f.close()


df.to_csv("executive_prep.csv", index=False, columns=columns_idex)



