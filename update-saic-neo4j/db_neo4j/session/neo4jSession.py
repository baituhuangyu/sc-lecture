#!/usr/bin/env python
# encoding: utf-8


import sys
from config.config import Neo4jConf
from neo4j.v1 import GraphDatabase

reload(sys)
sys.setdefaultencoding('utf8')

NEO4J_DIVER = GraphDatabase.driver(Neo4jConf.uri, auth=(Neo4jConf.userName, Neo4jConf.password))

