#!/usr/bin/env python
# encoding: utf-8


import sys
import re


reload(sys)
sys.setdefaultencoding('utf8')


def clean_position(s):
    if not s or len(s) > 200:
        return ""

    return re.sub("<.*?>", "", s)

