#!/usr/bin/env bash


/opt/neo4j-community-3.3.1/bin/neo4j-admin \
import \
--ignore-duplicate-nodes=true \
--ignore-missing-nodes==true \
--database=list_compant.db \
--id-type string \
--nodes:executive /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/executive.csv \
--nodes:stock /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/stock.csv \
--nodes:concept /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/concept.csv \
--nodes:industry /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/industry.csv \
--relationships:executive_stock /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/executive_stock.csv \
--relationships:stock_industry /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/stock_industry.csv \
--relationships:stock_concept /home/huangyu/sc-git-2/big-data/risk-model/saic-clean/xx/stock_concept.csv