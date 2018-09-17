#!/usr/bin/env python
# encoding: utf-8


import sys
from db_neo4j.get_neo4j_data import Neo4jSessionWrapper
from saic_format.saic_check import SaicCheck
from utils.md5_eid_pid import gen_md5_eid, gen_md5_pid
from saic_format.formaters.filter_share import judge_com
import copy

reload(sys)
sys.setdefaultencoding('utf8')


def compare_2dict_by_key_v_map(d_to_check, d_new, key_v_map):
    """
    key_v_map: key 为 d_to_check 的key, v 为 d_new 的key
    """
    for k, v in key_v_map.items():
        if d_to_check.get(k) != d_new.get(v):
            return False

    return True


def update_dict_by_keys(d_to_update, d_new, key_v_map):
    """
    key_v_map: key 为 d_to_update 的key, v 为 d_new 的key
    """
    for k, v in key_v_map.items():
        if d_new.get(v):
            d_to_update.update(**{k: d_new.get(v)})


class CompareSaicNeo4j(object):
    reps_all = [
        "CompanyInvestToCompany",
        "CompanyInvestToCompanyHistory",
        "CompanyPartnerCompany",
        "CompanyPartnerCompanyHistory",
        "PersonInvestToCompany",
        "PersonInvestToCompanyHistory",
        "PersonLegal",
        "PersonLegalHistory",
        "PersonPosition",
        "PersonPositionHistory",
    ]

    def __init__(self, saic):
        self.saic_obj = SaicCheck(saic)
        self.saic_obj.format_all()

        self.saic = saic
        self.basic = self.saic_obj.basic
        self.company = self.saic_obj.company
        self.person_list = self.saic_obj.person_list
        self.shareHolderList = self.saic_obj.shareHolderList

        self.session = Neo4jSessionWrapper()

        self.neo4j_relationship_data = dict()

    @staticmethod
    def new_fr_map(name):
        return {
            "PersonLegal": {"position": u"法人"},
            "node": {
                "type": "Person",
                "properties": {
                    "person": name,
                },
            }
        }

    @staticmethod
    def new_position_map(position, name):
        return {
            "PersonPosition": {"position": position, },
            "node": {
                "type": "Person",
                "properties": {
                    "person": name,
                },
            }
        }

    def parse_saic_to_map(self):
        """
        公司名字都用 name clean做清洗，清洗完之后作为key值。可能这个key跟原始值有差异。update neo4j的时候不 update 原始名称
        :return:
        """
        self.saic_node_map = {}

        # 法人
        if len(self.basic["frName"]) >= 2:
            self.saic_node_map[self.basic["frName"]] = self.new_fr_map(self.basic["frName"])

        for item in self.person_list:
            name = item["name"]
            tmp = self.saic_node_map.get(name, {})
            tmp.update(**self.new_position_map(item["position"], name))
            self.saic_node_map[item["name"]] = tmp

        for item in self.shareHolderList:
            if not any(item.values()):
                continue
            shareholderName = item["shareholderName"]
            tmp = self.saic_node_map.get(shareholderName, {})
            tmp.update(**{"share": item})
            # share 不做结构改变，因为类型不定，
            self.saic_node_map[shareholderName] = tmp

    @staticmethod
    def parse_neo4j_1_layer_path(neo4j_data):
        """
        因为只有一层，目前结构不用图结构, 用简单的node map 表示
        :return:
        """
        rst_dict = dict()
        for recode in neo4j_data:
            tmp = dict()
            if recode.values():
                map(lambda _: tmp.update(**{_.type: _.properties}), recode.values()[0].relationships)
                end_tmp = recode.values()[0].end
                node_type = list(end_tmp.labels)[0] if end_tmp.labels else ""
                tmp.update(**{
                    "node": {
                        "type": node_type,
                        "properties": end_tmp.properties,
                    }})
                if node_type == "Company":
                    if end_tmp.properties["companyName"] in rst_dict:
                        tmp.update(**rst_dict[end_tmp.properties["companyName"]])
                    rst_dict[end_tmp.properties["companyName"]] = tmp

                elif node_type == "Person":
                    if end_tmp.properties["person"] in rst_dict:
                        tmp.update(**rst_dict[end_tmp.properties["person"]])
                    rst_dict[end_tmp.properties["person"]] = tmp

        return rst_dict

    def merge_saic_neo4j_data_now(self):
        if not self.neo4j_relationship_data:
            return

        for node_name, info in self.neo4j_relationship_data.items():
            saic_node_info_tmp = self.saic_node_map.get(node_name, {})
            # 如果存在
            if saic_node_info_tmp:
                for key, relationship in saic_node_info_tmp.items():
                    if key == "node":
                        continue
                    # 如果是任职
                    elif key in ["PersonLegal", "PersonPosition"]:
                        if not relationship["position"]:
                            relationship.pop("position")
                        info.update(**{key: relationship})
                    # 如果是投资
                    elif key == "share":
                        tmp_dict = {
                                "invConum": relationship.get("subConam", ""),
                                "regCapCur": relationship.get("regCapCur", ""),
                                "conDate": relationship.get("conDate", ""),
                                "ratio": relationship.get("fundedRatio", ""),
                        }
                        tmp_dict = dict([(k, v) for k, v in tmp_dict.items() if v])
                        if info["node"]["type"] == "Company":
                            if "CompanyInvestToCompany" in info:
                                info["CompanyInvestToCompany"].update(**tmp_dict)
                            elif "CompanyPartnerCompany" in info:
                                info["CompanyPartnerCompany"].update(**tmp_dict)
                            else:
                                info["CompanyInvestToCompany"] = tmp_dict
                        elif info["node"]["type"] == "Person":
                            if "PersonInvestToCompany" in info:
                                info["PersonInvestToCompany"].update(**tmp_dict)
                            else:
                                info["PersonInvestToCompany"] = tmp_dict
                # 点在，关系不在了。
                if "PersonLegal" in info.keys() and "PersonLegal" not in saic_node_info_tmp.keys():
                    info["PersonLegalHistory"] = info.pop("PersonLegal")
                if "PersonPosition" in info.keys() and "PersonPosition" not in saic_node_info_tmp.keys():
                    info["PersonPositionHistory"] = info.pop("PersonPosition")
                if "share" not in saic_node_info_tmp.keys():
                    if "PersonInvestToCompany" in info.keys():
                        info["PersonInvestToCompanyHistory"] = info.pop("PersonInvestToCompany")
                    elif "CompanyPartnerCompany" in info.keys():
                        info["CompanyPartnerCompanyHistory"] = info.pop("CompanyPartnerCompany")
                    elif "CompanyInvestToCompany" in info.keys():
                        info["CompanyInvestToCompanyHistory"] = info.pop("CompanyInvestToCompany")
            # 点现在没有，以前有，把neo4j库里面的改为历史关系
            else:
                [info.update(**{_+"History": info.pop(_)}) for _ in info.keys() if _ != "node" and "History" not in _]

    def merge_saic_neo4j_data_new(self):
        if not self.saic_node_map:
            return

        for node_name, info in self.saic_node_map.items():
            # 新节点新关系
            tmp_dict = self.neo4j_relationship_data.get(node_name, {})
            if not tmp_dict:
                if "PersonLegal" in info:
                    tmp_dict.update(**self.new_fr_map(node_name))
                    tmp_dict["node"]["properties"]["personId"] = gen_md5_pid(self.company+node_name)
                    tmp_dict["node"]["type"] = "Person"
                if "PersonPosition" in info:
                    tmp_dict.update(**self.new_position_map(info["PersonPosition"]["position"], node_name))
                    tmp_dict["node"]["properties"]["personId"] = gen_md5_pid(self.company + node_name)
                    tmp_dict["node"]["type"] = "Person"
                if "share" in info:
                    tmp_share = info["share"]
                    share_name = tmp_share["shareholderName"]
                    tmp_share_dict = {
                        "invConum": tmp_share.get("subConam", "") or "0.000000",
                        "regCapCur": tmp_share.get("regCapCur", ""),
                        "conDate": tmp_share.get("conDate", ""),
                        "ratio": tmp_share.get("fundedRatio", ""),
                    }
                    tmp_share_dict = dict([(k, v) for k, v in tmp_share_dict.items() if v])
                    # 个人
                    tmp_dict["node"] = tmp_dict.get("node", {})
                    tmp_dict["node"]["properties"] = tmp_dict["node"].get("properties", {})
                    if len(share_name) < 5 or not judge_com(share_name):
                        tmp_dict.update(**{"PersonInvestToCompany": tmp_share_dict})
                        tmp_dict["node"]["type"] = "Person"
                        tmp_dict["node"]["properties"]["personId"] = gen_md5_pid(self.company + share_name)
                        tmp_dict["node"]["properties"]["person"] = share_name
                    else:
                        tmp_dict.update(**{"CompanyInvestToCompany": tmp_share_dict})
                        # 注释掉以下代码，因为暂时不考虑股东公司不存在的情况。这个不是很确定。
                        # tmp_dict["node"]["type"] = "Company"
                        # tmp_dict["node"]["properties"]["companyName"] = node_name
                        # tmp_dict["node"]["properties"]["companyId"] = gen_md5_pid(node_name)
            self.neo4j_relationship_data[node_name] = tmp_dict

    def update_company_node(self):
        """
        更新公司节点
        """
        params = {"typeName": "Company", "keyName": "companyName", "value": self.company}
        data = self.session.get_node_only_info_common(**params)
        if data:
            neo4j_data = data[0].data().values()[0].properties
        else:
            neo4j_data = self.create_company_node()
        basic_to_check_keys = [u'status', u'regCap', u'auditDate', u'regCapCur', u'esDate']
        if neo4j_data.get(u'companyName') != self.basic.get(u'enterpriseName'):
            return
        if not compare_2dict_by_key_v_map(neo4j_data, self.basic, dict(zip(basic_to_check_keys, basic_to_check_keys))):
            return

        update_dict_by_keys(neo4j_data, self.basic, dict(zip(basic_to_check_keys, basic_to_check_keys)))
        neo4j_data["typeName"] = "Company"
        neo4j_data["idName"] = "companyName"
        self.session.merge_node_info(**neo4j_data)

    def get_neo4j_relationship_data(self):
        params = {
            "sourceTypeName": "Company",
            "sourceKeyName": "companyName",
            "sourceValue": self.company,
            "relationship": "|".join(self.reps_all),
        }
        recodes = self.session.get_nodes_by_node_common(**params)
        self.neo4j_relationship_data = self.parse_neo4j_1_layer_path(recodes)
        self.neo4j_relationship_data_bak = copy.deepcopy(self.neo4j_relationship_data)

    def create_company_node(self):
        eid = gen_md5_eid(self.company)
        return {
            "companyId": eid,
            "companyName": self.company,
            "regCap": self.basic["regCap"],
            "regCapCur": self.basic["regCapCur"],
            "esDate": self.basic["esDate"],
            "status": self.basic["status"],
            "auditDate": self.basic["auditDate"],
        }

    def run(self):
        # 第一步update 或者create新公司节点
        self.update_company_node()
        # 第二步更新neo4j已有点上的已有关系和创建关系，不创建新点
        self.get_neo4j_relationship_data()
        self.parse_saic_to_map()

        # 将差集先保存起来做3层循环的查找节点，如果查到了更改id（pid、eid）和类型。
        neo4j_nodes = self.neo4j_relationship_data.keys()
        saic_nodes = self.saic_node_map.keys()
        new_nodes = set(saic_nodes) - set(neo4j_nodes)

        self.merge_saic_neo4j_data_now()

        for node_name, info in self.neo4j_relationship_data.items():
            tmp_node = info.get("node", {})
            if not tmp_node:
                continue
            target_type_name = tmp_node["type"]
            if target_type_name == "Company":
                target_key_name = "companyId"
                target_value = tmp_node["properties"].get(target_key_name, "")
            elif target_type_name == "Person":
                target_key_name = "personId"
                target_value = tmp_node["properties"].get(target_key_name, "")
            else:
                continue

            if not target_value:
                continue

            for relationship, p in info.items():
                if relationship == "node" or not p:
                    continue
                # 历史关系和现有关系都在，删除历史关系，更新现有关系
                if "History" not in relationship and relationship+"History" in info or ("History" in relationship and relationship.split("History")[0] in info):
                    ooold_rep = relationship.split("History")[0]
                    properties = copy.deepcopy(p)
                    properties.update({
                        "sourceTypeName": "Company",
                        "sourceKeyName": "companyName",
                        "sourceValue": self.company,
                        "oldRelationshipName": ooold_rep + "History",
                        "targetTypeName": target_type_name,
                        "targetKeyName": target_key_name,
                        "targetValue": target_value,
                        "newRelationshipName": ooold_rep,
                    })
                    self.session.update_relationship_type_common(**properties)
                # 更新或创建现在关系
                elif "History" not in relationship:
                    # create
                    if not self.neo4j_relationship_data_bak.get(node_name, {}).get(relationship, {}):
                        properties = copy.deepcopy(p)
                        properties.update({
                            "sourceTypeName": "Company",
                            "sourceKeyName": "companyName",
                            "sourceValue": self.company,
                            "relationshipName": relationship,
                            "targetTypeName": target_type_name,
                            "targetKeyName": target_key_name,
                            "targetValue": target_value,
                        })
                        self.session.create_relationship_2nodes(**properties)
                    # 更新现有关系
                    else:
                        properties = copy.deepcopy(p)
                        properties.update({
                            "sourceTypeName": "Company",
                            "sourceKeyName": "companyName",
                            "sourceValue": self.company,
                            "oldRelationshipName": relationship,
                            "targetTypeName": target_type_name,
                            "targetKeyName": target_key_name,
                            "targetValue": target_value,
                            "newRelationshipName": relationship,
                        })
                        self.session.update_relationship_type_common(**properties)

                # 更新历史关系
                else:
                    # 将现有关系变为历史关系
                    relationship_data_bak = self.neo4j_relationship_data_bak.get(node_name, {})
                    if relationship not in relationship_data_bak and relationship.split("History")[0] in relationship_data_bak:
                        properties = copy.deepcopy(p)
                        properties.update({
                            "sourceTypeName": "Company",
                            "sourceKeyName": "companyName",
                            "sourceValue": self.company,
                            "oldRelationshipName": relationship.split("History")[0],
                            "targetTypeName": target_type_name,
                            "targetKeyName": target_key_name,
                            "targetValue": target_value,
                            "newRelationshipName": relationship,
                        })
                    else:
                        properties = copy.deepcopy(p)
                        properties.update({
                            "sourceTypeName": "Company",
                            "sourceKeyName": "companyName",
                            "sourceValue": self.company,
                            "oldRelationshipName": relationship,
                            "targetTypeName": target_type_name,
                            "targetKeyName": target_key_name,
                            "targetValue": target_value,
                            "newRelationshipName": relationship,
                        })
                    self.session.update_relationship_type_common(**properties)
        # 第三步,添加新点，新的关系，先添加新点然后添加新关系，每个点都要检查一遍3层网络有没有这个点。公司类型的先做，然后在做人类型的点。
        self.merge_saic_neo4j_data_new()
        new_nodes_type = [(_, self.neo4j_relationship_data.get(_, {}).get("node", {}).get("type")) for _ in new_nodes]
        new_nodes_com = [_[0] for _ in new_nodes_type if _[1] == "Company"]
        new_nodes_person = [_[0] for _ in new_nodes_type if _[1] == "Person"]

        # 公司类型的点
        for n in new_nodes_com:
            node_info = self.neo4j_relationship_data[n]
            data = self.session.get_node_only_info_common(**{"typeName": "Company", "keyName": "companyName", "value": n})
            if data:
                company_id = data[0].data().values()[0].properties["companyId"]
            else:
                company_id = node_info["node"]["properties"]["companyId"]
                node_params = {
                    "typeName": "Company",
                    "idName": "companyId",
                    "companyId": company_id,
                }
                node_params.update(**(node_info["node"]["properties"]))
                self.session.merge_node_info(**node_params)
            for relationship, p in node_info.items():
                if not p:
                    continue
                if relationship != "node":
                    properties = copy.deepcopy(p)
                    properties.update({
                        "sourceTypeName": "Company",
                        "sourceKeyName": "companyName",
                        "sourceValue": self.company,
                        "relationshipName": relationship,
                        "targetTypeName": "companyName",
                        "targetKeyName": "companyId",
                        "targetValue": company_id,
                    })
                    self.session.create_relationship_2nodes(**properties)
        # 人类型的节点
        for n in new_nodes_person:
            node_info = self.neo4j_relationship_data[n]
            req_params = {
                "sourceTypeName": "Company",
                "sourceKeyName": "companyName",
                "sourceValue": self.company,
                "relationshipName": "|".join(self.reps_all),
                "targetTypeName": "Person",
                "targetKeyName": "person",
                "targetValue": n,
            }
            data = self.session.check_3layer_2nodes_has_relationship_common(**req_params)
            if data:
                person_id = data[0].data().values()[0].properties["personId"]
            else:
                person_id = node_info["node"]["properties"]["personId"]
                node_params = {
                    "typeName": "Person",
                    "idName": "personId",
                    "personId": person_id,
                }
                node_params.update(**(node_info["node"]["properties"]))
                self.session.merge_node_info(**node_params)
            for relationship, p in node_info.items():
                properties = copy.deepcopy(p)
                if not p:
                    continue
                if relationship != "node":
                    properties.update({
                        "sourceTypeName": "Company",
                        "sourceKeyName": "companyName",
                        "sourceValue": self.company,
                        "relationshipName": relationship,
                        "targetTypeName": "Person",
                        "targetKeyName": "personId",
                        "targetValue": person_id,
                    })
                    self.session.create_relationship_2nodes(**properties)

    def update_person_position(self):
        """
        更新任职
        :return:
        """
        for i, v in enumerate(self.person_list):
            self.session.get_relationship_common()

    def update_person_fr(self):
        """
        更新法人
        """

    def update_person_inv_company(self):
        """
        更新人投资公司
        :return:
        """

    def update_company_inv_company(self):
        """
        更新人投资公司
        :return:
        """

    def update_company_partner_company(self):
        """
        更新公司合伙公司
        :return:
        """
