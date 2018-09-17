#!/usr/bin/env python
# encoding: utf-8


import sys
from utils.utils import name_clean, parse_date, parse_money_six
from saic_format.formaters.parse_reg_cap_cur import parse_cap_cur_to_code
from saic_format.formaters.ent_status_mapping import parse_ent_status_code
from saic_format.formaters.filter_share import filter_inv_name
from saic_format.formaters.clean_position import clean_position

reload(sys)
sys.setdefaultencoding('utf8')


class SaicCheck(object):
    basic_to_check_keys = ["enterpriseName", "frName", "regCap", "regCapCur", "status", "auditDate", "esDate"]
    share_to_check_keys = ["shareholderName", "subConam", "regCapCur", "conDate"]
    person_to_check_keys = ["position", "name"]

    def __init__(self, saic):
        if not saic or not saic.get("basicList") or not saic.get("basicList")[0]:
            raise Exception("saic error")

        self.saic = saic
        self.basic = saic.get("basicList", [{}])[0]
        self.company = self.basic.get("enterpriseName", "") or ""

        if len(self.company) < 3:
            raise Exception("saic companyName error")

        self.person_list = saic.get("personList", []) or []
        self.shareHolderList = saic.get("shareHolderList", []) or []
        self.person_map = {}
        self.share_map = {}

    def check_basic(self):
        """
        检查字段存在问题， 以及mapping
        """
        self.basic = dict([(_, self.basic.get(_)) for _ in self.basic_to_check_keys])
        # todo

    def check_share(self):
        self.shareHolderList = [dict([(k, item.get(k)) for k in self.share_to_check_keys]) for item in self.shareHolderList]
        self.shareHolderList = [_ for _ in self.shareHolderList if any(_.values()) and not filter_inv_name(_.get("shareholderName"))]

    def check_person(self):
        self.person_list = [dict([(k, item.get(k)) for k in self.person_to_check_keys]) for item in self.person_list]
        self.person_list = [_ for _ in self.person_list if any(_.values())]

    def format_basic(self):
        self.check_basic()

        basic_to_check_keys_fun = {
            "enterpriseName": name_clean,
            "frName": name_clean,
            "regCap": parse_money_six,
            "regCapCur": parse_cap_cur_to_code,
            "status": parse_ent_status_code,
            "auditDate": parse_date,
            "esDate": parse_date,
        }

        for k, f in basic_to_check_keys_fun.items():
            self.basic[k] = f(self.basic[k])

        if len(self.basic["frName"]) < 2:
            self.basic["frName"] = ""

    def format_share(self):
        self.check_share()

        share_to_check_keys_func = {
            "shareholderName": name_clean,
            "subConam": parse_money_six,
            "regCapCur": parse_cap_cur_to_code,
            "conDate": parse_date,
        }

        for i, item in enumerate(self.shareHolderList):
            for k, f in share_to_check_keys_func.items():
                self.shareHolderList[i][k] = f(item[k])

        total = 0
        for _, item in enumerate(self.shareHolderList):
            a_subConam = item.get("subConam", "")
            try:
                a_subConam = float(a_subConam)
            except:
                a_subConam = 0.0
            total += a_subConam

        for i, item in enumerate(self.shareHolderList):
            a_rotio = 0
            try:
                a_subConam = item.get("subConam", "")
                a_subConam = float(a_subConam)
                a_rotio = a_subConam * 1.0 / total
            except:
                pass

            self.shareHolderList[i]["fundedRatio"] = "%.4f" % a_rotio if a_rotio else ""

        self.shareHolderList = filter(lambda _: len(_.get("shareholderName")) >= 2, self.shareHolderList)

    def format_person(self):
        self.check_person()

        person_to_check_keys_func = {
            "position": clean_position,
            "name": name_clean,
        }
        for i, item in enumerate(self.person_list):
            for k, f in person_to_check_keys_func.items():
                self.person_list[i][k] = f(item[k])

        self.person_list = filter(lambda _: len(_.get("name")) >= 2, self.person_list)

    def format_all(self):
        self.format_basic()
        self.format_person()
        self.format_share()

    def build_person_map(self):
        self.person_map = dict([(_.get("name"), _) for _ in self.person_list if _.get("name")])

    def build_share_map(self):
        self.share_map = dict([(_.get("shareholderName"), _) for _ in self.shareHolderList if _.get("shareholderName")])

    def build_map_all(self):
        self.build_person_map()
        self.build_share_map()


