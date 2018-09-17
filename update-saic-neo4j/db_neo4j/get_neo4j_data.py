#!/usr/bin/env python
# encoding: utf-8


import sys
from db_neo4j.session.neo4jSession import NEO4J_DIVER
import copy
import json

reload(sys)
sys.setdefaultencoding('utf8')


class Neo4jSessionWrapper(object):
    def __init__(self):
        self.session = NEO4J_DIVER.session()

    def get_neo4j_recodes(self, cql, **params):

        records = []
        with self.session.begin_transaction() as tx:
            for record in tx.run(cql, **params):
                records.append(record)

        return records

    def execute_none_result(self, cql, is_exe=False, **params):
        print "=" * 50
        print "cql", cql
        print "params", json.dumps(params, indent=4, ensure_ascii=False)
        print "=" * 50

        if is_exe:
            with self.session.begin_transaction() as tx:
                print tx.run(cql, **params)
        else:
            # print cql
            pass

    @staticmethod
    def check_params(*to_check_keys, **raw_params):
        need_params = [raw_params.get(_) for _ in to_check_keys]
        if not all(need_params):
            raise Exception("params err")

    def get_node_only_info_common(self, **params):
        cql = "match (n:%s {%s: {value}}) return n limit 1;"
        keys = ["typeName", "keyName", "value"]
        self.check_params(*keys, **params)
        cql_format = cql % tuple([params.get(_) for _ in keys[:-1]])

        return self.get_neo4j_recodes(cql_format, **params)

    def get_relationship_common(self, **params):
        cql = """match
        p=(n:%s {%s: {sourceValue}})<-[*0..1]-(x:%s {%s: {targetValue}})
        return p limit 2000;"""
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "targetTypeName", "targetKeyName", "targetValue"]
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")])
        self.check_params(*keys, **params)
        return self.get_neo4j_recodes(cql_format, **params)

    def get_nodes_by_node_common(self, **params):
        cql = """match
        p=(n:%s {%s: {sourceValue}})<-[:%s]-(x)
        return p limit 2000;"""
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "relationship"]
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")])
        self.check_params(*keys, **params)
        return self.get_neo4j_recodes(cql_format, **{"sourceValue": params.get("sourceValue")})

    def get_other_node_by_node_relationship_common(self, **params):
        cql = """match (n:%s {%s: {sourceValue}})<-[:%s]-(x:%s {%s: {targetValue}})
        return x limit 2000;"""
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "relationshipName", "targetTypeName", "targetKeyName", "targetValue"]
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")])
        self.check_params(*keys, **params)
        return self.get_neo4j_recodes(cql_format, **params)

    def check_3layer_2nodes_has_relationship_common(self, **params):
        cql = """match (n:%s {%s: {sourceValue}})<-[:%s*0..3]-(x:%s {%s: {targetValue}})
        return x limit 1;"""
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "relationshipName", "targetTypeName", "targetKeyName", "targetValue"]
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")])
        self.check_params(*keys, **params)
        return self.get_neo4j_recodes(cql_format, **params)

    def merge_node_info(self, **params):
        property_params = copy.deepcopy(params)

        if "typeName" not in params or "idName" not in params or not params.get(params.get("idName")):
            return

        property_params.pop("typeName")
        property_params.pop("idName")

        property_str = ",".join(["n.%s={%s}" % (k, k) for k in property_params])
        cql = """
        MERGE (n:%s {%s:{%s}})
        ON CREATE SET %s
        ON MATCH SET %s
        RETURN n;
        """
        cql_format = cql % (params.get("typeName"), params.get("idName"), params.get("idName"), property_str, property_str)
        # exit(0)

        self.execute_none_result(cql_format, is_exe=True, **property_params)

    def update_relationship_type_common(self, **params):
        cql = """
        match (n:%s {%s: {sourceValue}})<-[oldr:%s]-(x:%s {%s:{targetValue}})
        create (n)<-[newr:%s]-(x)
        set %s
        with oldr
        delete oldr
        """
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "oldRelationshipName", "targetTypeName", "targetKeyName",
                "targetValue", "newRelationshipName"]
        set_property_list = ["newr.%s={%s}" % (k, k) for k in params if k not in keys]
        if not set_property_list:
            return
        set_property = ",".join(set_property_list)
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")] + [set_property])
        self.check_params(*keys, **params)
        return self.execute_none_result(cql_format, is_exe=True, **params)

    def update_relationship_property_common(self, **params):
        cql = """
        match (n:%s {%s: {sourceValue}})<-[r:%s]-(x:%s {%s:{targetValue}})
        set %s
        """
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "relationshipName", "targetTypeName", "targetKeyName", "targetValue"]
        set_property_list = ["r.%s={%s}" % (k, k) for k in params if k not in keys]
        if not set_property_list:
            return
        set_property = ",".join(set_property_list)
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")]+[set_property])
        self.check_params(*keys, **params)
        return self.execute_none_result(cql_format, is_exe=True, **params)

    def create_relationship_2nodes(self, **params):
        cql = """
        match (n:%s {%s: {sourceValue}}), (x:%s {%s:{targetValue}})
        create (n)<-[r:%s]-(x)
        set %s
        """
        keys = ["sourceTypeName", "sourceKeyName", "sourceValue", "targetTypeName", "targetKeyName",
                "targetValue", "relationshipName"]
        set_property_list = ["r.%s={%s}" % (k, k) for k in params if k not in keys]
        if not set_property_list:
            return
        set_property = ",".join(set_property_list)
        cql_format = cql % tuple([params.get(_) for _ in keys if not _.endswith("Value")] + [set_property])
        self.check_params(*keys, **params)
        return self.execute_none_result(cql_format, is_exe=True, **params)







