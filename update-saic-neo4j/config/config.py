#!/usr/bin/env python
# encoding: utf-8


import sys

reload(sys)
sys.setdefaultencoding('utf8')


class Neo4jConf(object):
    uri = "bolt://"
    userName = ""
    password = ""


class SaicConf(object):
    url = ""


class UpdatQueueConf(object):
    queue_name = "saic_update_neo4j"
    # queue_name = "saic_update_neo4j_offline"


class AwsConf(object):
    aws_access_key_id = ""
    aws_secret_access_key = ""
    aws_region_name = ""


####

class SaicQuantumConf(object):
    regist_url = ""
    share_url = ""
    manager_url = ""


class UpdatQueueQuantumConf(object):
    queue_name = ""
    # queue_name = ""


model_dx = ""
model_quantum = ""




