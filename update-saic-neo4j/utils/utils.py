#!/usr/bin/env python
# encoding: utf-8


import re
import sys
from scpy.xawesome_time import parse_time
import time
import traceback
reload(sys)
sys.setdefaultencoding('utf8')


REGEX = u'[^\u2E80-\u9FFFa-zA-Z0-9\uFF10-\uFF19\uFF41-\uFF5a()（）]'
PATTERN = re.compile(REGEX)


def parse_date(time_str):
    if not time_str:
        return ""
    s_time = parse_time(time_str)
    data_time = s_time[0:10] if len(s_time) > 10 else ""
    return data_time


def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        # 全角空格直接转换
        if inside_code == 12288:
            inside_code = 32
        # 全角字符（除空格）根据关系转化
        elif 65374 >= inside_code >= 65281:
            inside_code -= 65248

        rstring += unichr(inside_code)
    return rstring


def name_clean(company_name):
    company_name = PATTERN.sub('', strQ2B(unicode(company_name)))
    return change_blank(company_name)


def change_blank(name):
    return name.replace(u"(", u"（").replace(u")", u"）")


def _parse_money_six(money):
    if money == "" or money == "企业选择不公示":
        return money
    if isinstance(money, int) or isinstance(money, float):
        money = '%f' % money
    elif isinstance(money, basestring):
        money = str(money)
    else:
        return money
    if re.findall('-*^[^\d]*\.\d', money):
        money = money.replace('.', '0.')
    money = money.replace(",", "")
    number = re.findall('-*\d+', money)
    if len(number) == 0:
        return '0.000000'
    elif len(number) == 1:
        number.append('000000')
    else:
        lst_num = number[-1]
        if len(lst_num) >= 6:
            lst_num = lst_num[:6]
        else:
            lst_num += '0' * (6-len(lst_num))
        number[-1] = lst_num

    # 多个小数点，清洗变为空
    if len(number) > 2:
        return ''

    return '.'.join(number)


def parse_money_six(money):
    try:
        return _parse_money_six(money)
    except KeyboardInterrupt:
        return KeyboardInterrupt
    except Exception as e:
        print e
        return money


def tail_number(company_name):
    pattern = ur"[\u4e00-\u9fa5]*[\d]+$"
    res = re.match(pattern, company_name)
    return res


def tail_char(company_name):
    pattern = ur"[\u4e00-\u9fa5]*[a-zA-Z]+$"
    res = re.match(pattern, company_name)
    return res


def _parse_name_legal_check(a_name):
    """
    公司名合法性校验（公司名合法 -> True）
    :param a_name:
    :return:
    """
    _all_num_pattern = re.compile("\d")
    _all_char_pattern = re.compile("\w")

    a_name_upper = a_name.upper()
    if a_name_upper.endswith(u"OK") or a_name_upper.endswith(u"KTV"):
        return True

    if not _all_num_pattern.sub("", a_name):
        return False

    if len(a_name) < 10 and len(_all_char_pattern.sub("", a_name)) < 2:
        return False

    if len(set(a_name.split())) < 4:
        return False

    return True


def _parse_company_name(a_name):
    return tail_number(a_name) or tail_char(a_name)


def _legal_check(query, min_len=6):
    """
    公司名合法性校验（公司名合法 -> True）
    """

    # 字符串长度 < min_len
    cond_0 = bool(len(query) < min_len)
    # 全英文
    cond_1 = not bool(re.search(u'[^a-zA-Z]', query))
    # 数字占比 >= 0.6
    cond_2 = bool(float(len(re.findall(u'[0-9]', query))+1) / (len(query)+1) >= 0.75)
    # 包含特殊字符
    cond_3 = bool(re.search(u'[~!@#$%\^&\*\-_=+<>,;:、。?/\\|～＠＃￥％＾＆＊＿＝＋＜＞，．。；：？／＼｜]', query))
    # 数字在前面的错误情况
    cond_4 = bool(re.match(u'[\(（]?\d{1,2}[\.\)）\u2E80-\u9FFF\s]', query))
    # 数字在最后的错误情况
    cond_5 = bool(re.search(u'[\u2E80-\u9FFF]+[\da-zA-Z]?[\d]$', query))
    return not (cond_0 or cond_1 or cond_2 or cond_3 or cond_4 or cond_5)


def retry_times_sleep(max_time):

    def decorator(func):
        def wrapper(*args, **kwargs):
            max_try = 0
            __e = None
            while max_try < max_time:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as __e:
                    time.sleep(2)
                    max_try += 1
                    continue
            if __e:
                traceback.print_exc(__e)
            raise Exception("Repeated attempts are still wrong")

        return wrapper

    return decorator
