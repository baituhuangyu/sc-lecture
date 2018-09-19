# neo4j 网络图
## neo4j 库相关
## 数据库相关
- 线下数据库IP 
- 线上数据库IP 
- 网页浏览(线下):  
- 网页浏览(线上):  
- Blot协议地址(线下): 
- Blot协议地址(线上): 
- 数据库Doc： https://neo4j.com/developer/get-started/
- 简单版Cypher： https://neo4j.com/docs/developer-manual/current/cypher/

## 工商网络图
### 数据结构
- 数据由Node类型和Relationship两种类型构成。
- Node 由 Company 和 Person 构成，其他类型的暂时不考虑。 Company是公司类型的节点，Person是自然人类型（实际上是没有EID的节点，可能是自然人、境外公司、政府机构）
- Relationship 由 CompanyInvestToCompany、 CompanyPartnerCompany、 PersonInvestToCompany、 PersonLegal、 PersonPosition 几种构成。其他的暂时不考虑，所有关系都是有向的。

| 类型 | CompanyInvestToCompany | CompanyPartnerCompany | PersonInvestToCompany | PersonLegal | PersonPosition|
| ------ | ------ | ------ | ------ | ------ | ------ |
| 含义 | 公司投资公司 | A公司担任B公司的法人（合伙关系） | 人投资公司 | 人担任公司的法人（法人关系） | 人担任公司的高管（高管关系） |
| 方向 | 由投资公司指向被投资公司 | 由法人（公司）指向其他公司 | 由人指向被投资的公司 | 有法人指向公司 | 有高管指向公司 |

#### 公司节点的属性

| 字段名称 | companyId | companyName | esDate | auditDate | regCap | regCapCur | status |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| 数据类型 | String | String | String | String | String | String | String |
| 含义 | 公司ID | 公司名 | 成立时间 | 核准时间 | 注册资本 | 注册资本的币种[详见]() | 公司状态[详见]() |

#### 人节点数据类型
| 字段名称 | personId | person |
| ------ | ------ | ------ |
| 数据类型 | String | String |
| 含义 | 人ID | 人名 |

#### CompanyInvestToCompany、PersonInvestToCompany 投资类型的关系属性
| 字段名称 | invConum | ratio | regCapCur | conDate |
| ------ | ------ | ------ | ------ | ------ |
| 含义 | 认缴出资额 | 认缴出资比例 | 认缴出资币种（分母是注册资本） | 认缴出资时期 |


####  CompanyPartnerCompany、 PersonLegal、 PersonPosition 任职类型关系属性
| 字段名称 | position |
| ------ | ------ |
| 含义 | 担任的职位 |

#### index
- 节点属性中建了索引的字段有 companyId、personId、companyName、person
- 关系属性没有建索引


## Cypher Demo

### 查询企业
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'}) RETURN n
```

### 企业对外投资的企业名称（仅名称）
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})-[:CompanyInvestToCompany]->(x:Company) return x
```

### 企业对外投资的企业关系及属性（查询比例等）
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})-[:CompanyInvestToCompany]->(x:Company) return p
```

### 查询企业的法人
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonLegal]-(x:Person) return x
```

### 查询企业的高管
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonPosition]-(x:Person) return x
```

### 查询企业的股东（股东是公司）
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:CompanyInvestToCompany]-(x:Company) return x
```

### 查询企业的股东（股东是自然人）
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonInvestToCompany]-(x:Person) return x
```

### 查询企业的股东（所有股东）
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonInvestToCompany|:CompanyInvestToCompany]-(x) return x
```

### 查询所有股东并包含属性
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonInvestToCompany|:CompanyInvestToCompany]-(x) return p
```

### 查询公司法人对外担任法人的公司
```
MATCH (n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonLegal|:CompanyPartnerCompany]-(m)-[:PersonLegal|:CompanyPartnerCompany]->(x:Company) return x
```
包含关联关系属性
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonLegal|:CompanyPartnerCompany]-(m)-[:PersonLegal|:CompanyPartnerCompany]->(x:Company) return p
```

### 查询公司法人对外投资的公司
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[r1:PersonLegal|:CompanyPartnerCompany]-(m)-[r2:PersonInvestToCompany|:CompanyInvestToCompany]->(x:Company)  return p
```

### 查询公司所有股东对外投资的公司（包含属性）
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[:PersonInvestToCompany|:CompanyInvestToCompany]-(m)-[:PersonInvestToCompany|:CompanyInvestToCompany]->(x:Company) return p limit 100
```
- 注意limit 100 中的100 不是最后的公司数量，是路径的数量，路径的计算方式比较复杂，这里略过

### 查询公司所有股东对外投资的公司 比例筛选
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[r1:PersonInvestToCompany|:CompanyInvestToCompany]-(m)-[r2:PersonInvestToCompany|:CompanyInvestToCompany]->(x:Company) where r1.ratio>'0.1' and r2.ratio>'0.1' return p
```


### 查询公司董监高对外投资的公司
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[r1:PersonPosition]-(m)-[r2:PersonInvestToCompany|:CompanyInvestToCompany]->(x:Company)  return p
```

### 查询公司董监高对外担任法人的公司
```
MATCH p=(n:Company {companyName: '重庆誉存大数据科技有限公司'})<-[r1:PersonPosition]-(m)-[r2:PersonLegal]->(x:Company)  return p
```
