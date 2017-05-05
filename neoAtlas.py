#!/usr/bin/python
#makes a set of country-sub_region-region nodes in neo4j

import csv
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "icecream!"))
session = driver.session()


## Uncomment to blow away any existing data
## session.run("MATCH (n) DETACH DELETE n")
## CARE!

file = open ("world_list.txt","r")

contents = csv.reader(file, delimiter=',', quotechar='"')
for line in contents:
    [name,alpha2,alpha3,country_code,iso_3166_2,region,sub_region,region_code,sub_region_code] = line
    session.run("MERGE (c:Country{name: {name} , alpha2: {alpha2}, alpha3: {alpha3}})"
                "MERGE (sr:SUB_REGION{name: {sub_region}})"
                "MERGE (c) -[:IS_IN]->(sr)"
                "MERGE (r:REGION{name: {region}})"
                "MERGE (sr) -[:IS_IN]-> (r)"
                "MERGE (p:PLANET{name: {earth}})"
                "MERGE (r) -[:IS_IN]-> (p)", name = name, alpha2 = alpha2, alpha3 = alpha3, sub_region =sub_region, region = region, earth = "Earth")
    
