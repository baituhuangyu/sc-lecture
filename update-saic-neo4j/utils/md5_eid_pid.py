#!/usr/bin/env python
# encoding: utf-8


import sys
import hashlib
reload(sys)
sys.setdefaultencoding('utf8')


def gen_md5_eid(s):
    """
    s 公司名字 utf8编码
    """
    if not s:
        raise Exception("s cannot be void")

    m = hashlib.md5()
    if isinstance(s, unicode):
        s = s.encode("utf8")
    m.update(s)
    return "SCE"+m.hexdigest().upper()


def gen_md5_pid(s):
    """
    s 主体公司名字+人名 utf8编码
    """
    if not s:
        raise Exception("s cannot be void")

    m = hashlib.md5()
    if isinstance(s, unicode):
        s = s.encode("utf8")
    m.update(s)
    return "SCP"+m.hexdigest().upper()




