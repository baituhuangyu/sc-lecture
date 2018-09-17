#!/usr/bin/env python
# encoding: utf-8


import sys
import json
from utils.scaws import SQS
from config.config import UpdatQueueConf, UpdatQueueQuantumConf, model_dx, model_quantum
from scpy.logger import get_logger
from utils.utils import name_clean
from get_saic.get_saic import get_saic, get_saic_quantum
from cyphers.update_neo4j_data import CompareSaicNeo4j

reload(sys)
sys.setdefaultencoding('utf8')
logger = get_logger(__file__)


def get_msg(model=None):
    sqs = SQS()
    if model == model_dx:
        queue_name = UpdatQueueConf.queue_name
    elif model == model_quantum:
        queue_name = UpdatQueueQuantumConf.queue_name
    else:
        return

    msg = sqs.receive_message(queue_name=queue_name)
    if not msg:
        return
    logger.info("%s : %s" % (queue_name, msg))
    msg0 = json.loads(msg[0].get('Body')) if msg else {}
    if not msg0:
        # logger.info("corp no msg loop")
        return
    if "Message" in msg0:
        msg0 = msg0.get("Message", {}) or {}
    companyName = msg0.get('companyName', '') or msg0.get('name', '') or msg0.get('company_name', '')
    companyName = name_clean(companyName)
    logger.info("get companyName: %s", companyName)
    return companyName


def task_by_name(companyName, model=None):
    if len(companyName) < 5:
        return

    if model == model_dx:
        saic_data = get_saic(companyName)
    elif model == model_quantum:
        saic_data = get_saic_quantum(companyName)
    else:
        return

    if not saic_data:
        logger.info("no saic_data")
        return

    t = CompareSaicNeo4j(saic_data)
    t.run()
    logger.info("companyName: %s ok", companyName)


def task_by_q(model=None):
    companyName = get_msg(model)
    if companyName:
        task_by_name(companyName, model)



