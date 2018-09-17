#!/usr/bin/env python
# encoding: utf-8


import sys

reload(sys)
sys.setdefaultencoding('utf8')


ent_status_mapping = {
    u"在营": "1",
    u"开业": "1",
    u"在册": "1",
    u"在业": "1",
    u"存续": "1",
    u"续存": "1",
    u"注册": "1",
    u"正常": "1",

    u"吊销": "2",

    u"注销": "3",
    u"撤销": "3",
    u"已告解散": "3",

    u"迁出": "4",
    u"迁往市外": "4",
    u"迁移异地": "4",
    u"迁": "4",

    u"停业": "8",

    u"其他": "9",
    u"个体转企业": "9",
}


def parse_ent_status_code(s):
    if not s:
        return ""

    s_mp = ent_status_mapping.get(s, "")
    if s_mp:
        return s_mp

    # 注销类
    if u"未注销" in s and u"吊销" in s:
        return u"2"

    if u"未吊销" in s and u"注销" in s:
        return u"3"

    for k, v in ent_status_mapping.items():
        if k in s:
            return v

    return ""


