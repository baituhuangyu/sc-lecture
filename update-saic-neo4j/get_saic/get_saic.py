#!/usr/bin/env python
# encoding: utf-8


import sys
import requests
from config.config import SaicConf, SaicQuantumConf
from utils.utils import retry_times_sleep

reload(sys)
sys.setdefaultencoding('utf8')


@retry_times_sleep(10)
def get_saic(companyName):
    res = requests.get(SaicConf.url, params={"companyName": companyName}, timeout=10)
    if res.status_code == 200:
        return res.json().get("data") or {}
    else:
        return {}


@retry_times_sleep(10)
def get_saic_quantum(companyName):
    task_kv = [("basicList", SaicQuantumConf.regist_url),
               ("shareHolderList", SaicQuantumConf.share_url),
               ("personList", SaicQuantumConf.manager_url),
               ]

    result = {}

    for (k, url) in task_kv:

        resp = requests.get(url, params={"companyName": companyName}, timeout=10)
        if resp.status_code == 200:
            content = resp.json().get("data") or []
        else:
            if k == "basicList":
                return {}
            else:
                content = []

        result[k] = content

    return result
