#!/usr/bin/env python
# encoding: utf-8


import sys
from utils.scaws import SQS
from config.config import UpdatQueueQuantumConf

reload(sys)
sys.setdefaultencoding('utf8')


if __name__ == '__main__':
    with open("./company_quantum.csv", "r") as fp:
        while True:
            a_line = fp.readline()
            if not a_line:
                break
    #     a_line = "河南省京良饮料有限公司"
            print a_line
            # task_by_name(a_line.strip())
            sqs = SQS()
            msg = sqs.send_message({"companyName": a_line.strip()}, queue_name=UpdatQueueQuantumConf.queue_name)
