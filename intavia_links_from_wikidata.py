# -*- coding: utf-8 -*-

import os
import sys

from requests.auth import HTTPBasicAuth
import requests

from collections import defaultdict
from rdflib import Namespace, URIRef, Graph
from rdflib.namespace import RDF, XSD

import prefect
from prefect import Flow, task, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun

from SPARQLWrapper import SPARQLWrapper, JSON
import logging


"""Define some useful functions for data (SPARQL results in JSON to Python list of dictionaries) convertion"""
DATATYPECONVERTERS = {
      str(XSD.integer):  int,
      # str(XSD.date):     lambda v: datetime.datetime.strptime(v, '%Y-%m-%d').date()
      str(XSD.decimal):  float,
  }

def convertDatatype(obj):
  return DATATYPECONVERTERS.get(obj.get('datatype'), str)(obj.get('value')) 

def convertDatatypes(results):
    res = results["results"]["bindings"]
    return [dict([(k, convertDatatype(v)) for k,v in r.items()]) for r in res]


def wikilinkquery(urls = [], limit = None):
  '''Query relations between people. 
  Query example in yasgui: https://api.triplydb.com/s/G2uTtjzHW
  '''

  return """PREFIX bd: <http://www.bigdata.com/rdf#>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX schema: <http://schema.org/>
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  prefix wds: <http://www.wikidata.org/entity/statement/>
  PREFIX wdt: <http://www.wikidata.org/prop/direct/>
  PREFIX wd: <http://www.wikidata.org/entity/>
  PREFIX wikibase: <http://wikiba.se/ontology#> 
  
  SELECT DISTINCT ?actor1 ?actor1Label ?relation ?actor2 ?actor2Label
  WITH {
    # First query filter those with an links to any other person
    SELECT DISTINCT ?actor1
    WHERE {
      VALUES ?actor1 { """ + ' '.join((f'<{s}>' for s in urls)) + """ }
      # needs a link (of any kind) to any other person
      ?actor1 ?prop [ wdt:P31 wd:Q5 ]
    }
  } AS %actors
  WHERE {
  INCLUDE %actors
  # map properties used in wikidata to BioCRM:
  VALUES (?prop ?relation) {
    (wdt:P802 "student")
    (wdt:P1066  "teacher")
    (wdt:P1598 "consecrator")
    (wdt:P184 "doctoral_advisor")
    (wdt:P185 "doctoral_student")
    (wdt:P1327 "collaborator")
    (wdt:P3342 "significant_person")
    (wdt:P737 "influenced_by")
    (wdt:P451 "unmarried_partner")
    (wdt:P108 "employer")
    (wdt:P859 "sponsor")
    (wdt:P1290 "godparent")
    (wdt:P157 "killed_by")
    (wdt:P3448 "stepparent")

    (wdt:P22 "parent") # "father"
    (wdt:P25 "parent") # "mother"
    (wdt:P26 "spouse")
    (wdt:P40 "child")
    (wdt:P3373 "sibling")
  }
  ?actor1 ?prop ?actor2 .
  FILTER(?actor1 != ?actor2)
  ?actor2 wdt:P31 wd:Q5 .
  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  } """ + (f'LIMIT {limit}' if limit else '')

def queryDatabase(wrapper, urls, query,  wikilimit = 10000, limit = 20000):
  if len(urls)>10000:
    # split query into smaller chunks
    return queryDatabase(wrapper, urls[:wikilimit], query, wikilimit = wikilimit, limit=limit) + queryDatabase(wrapper, urls[wikilimit:], query, wikilimit = wikilimit, limit=limit)
  
  wrapper.setQuery(query(urls = urls, limit = limit))

  results = wrapper.query().convert()

  res = results["results"]["bindings"]
  print(f'Queried {len(urls)} with {len(res)} results')
  return convertDatatypes(results)

def queryWikidata(sparql, URLS):
  return queryDatabase(sparql, URLS, wikilinkquery, wikilimit = 10000)

@task
def setup_sparql_connection(endpoint):
    '''could be imported from entity_id_linker as well'''
    sparql = SPARQLWrapper(endpoint)  
    sparql.setReturnFormat(JSON)
    sparql.setMethod('POST')  
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql

@task
def setup_wd_sparql_connection(endpoint):
    '''could be imported from entity_id_linker as well'''
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setMethod('POST')
    return sparql

def queryIKBactors(sparql, limit = 60000):
  PREFIXES = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/> """

  sparql.setQuery(PREFIXES +
  """ SELECT DISTINCT ?id ?wiki
  WHERE {
    {
      VALUES ?cls { crm:E21_Person <http://www.intavia.eu/idm-core/Person_Proxy> }
      ?id owl:sameAs ?wiki ; a ?cls .
      FILTER(CONTAINS(STR(?wiki), 'wikidata.org/entity/Q')) # 29322 results
    }
    UNION {
      ?wiki <http://www.wikidata.org/prop/direct/651> ?id .
    }
  }""" + (f' LIMIT {limit}' if limit else ''))

  results = sparql.query().convert()
  return convertDatatypes(results)

@task
def create_wikidata_connections(intavia_sparql, wd_sparql, limit):
  logger = prefect.context.get('logger')

  intavias = queryIKBactors(intavia_sparql, limit = limit)

  logger.debug(f'Queried {len(intavias)} results from IKB')
  for ob in intavias[:10]: # 58868 results
    logger.debug(ob)

  # Check multiple wikidata links in bs actors
  
  check = defaultdict(list)
  for ob in intavias:
    check[ob.get('wiki')].append(ob.get('id'))
  
  LOOKUP_BY_WIKIURL = dict((k,arr[0]) for k, arr in check.items())

  URLS = [ob.get('wiki') for ob in intavias]

  links = queryWikidata(wd_sparql, URLS)

  # choose links where to target is also in InTaVia
  links = [ob for ob in links if ob.get('actor2') in URLS] 
  logger.debug(f"Filtered to {len(links)} results in InTaVia")

  '''
  31 Mar 2023, added family relations
  Altogether 46434 results
  Filtered to 18325 results in InTaVia
  '''

  """## add InTaVia URLs and data sources"""

  for ob in links:
    ob['url1'] = LOOKUP_BY_WIKIURL.get(ob.get('actor1'))
    ob['url2'] = LOOKUP_BY_WIKIURL.get(ob.get('actor2'))

  links.sort(key = lambda ob: (ob.get('url1'), ob.get('relation'), ob.get('url2')))


  BIOC = Namespace('http://ldf.fi/schema/bioc/')
  INTAVIA_RELATIONS = Namespace('http://www.intavia.eu/wd/personrelation/')
  INTAVIA_RELATION_CLASSES = Namespace('http://www.intavia.eu/personreltype/')

  # write rdf graph
  g = Graph()
  for i,row in enumerate(links):
    source, target = URIRef(row.get("url1")), URIRef(row.get("url2"))

    prop = INTAVIA_RELATION_CLASSES[row.get("relation")]
    rel_url = INTAVIA_RELATIONS[f'r{i}']
    g.add((source, BIOC.has_person_relation, rel_url))
    g.add((rel_url, RDF.type, prop))
    g.add((rel_url, BIOC.inheres_in, target))

  g.bind('bioc', BIOC)
  g.bind('personrelations', INTAVIA_RELATIONS)
  g.bind('relationtypes', INTAVIA_RELATION_CLASSES)
  return g
  
@task()
def serialize_data(environment, endpoint, target_uri, data, file):
    if environment == "production":
        update_target_graph(endpoint, target_uri, data)
    else:
        # for local dev
        write_graph_to_file(data, file)
        logger = prefect.context.get('logger')
        logger.debug(f'{len(data)} triples written to {file}')


def update_target_graph(endpoint, target_uri, data):
    logger = prefect.context.get('logger')
    delete_url =  endpoint + '?c=<' + target_uri + '>'
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    post_url =  endpoint + '?context-uri=' + target_uri + ''
    res = requests.delete(delete_url, auth=auth)
    res2 = requests.post(post_url, headers={'Content-type': 'text/turtle'}, data=data.serialize(), auth=auth)
    logger.info(res)
    logger.info(res2)

def write_graph_to_file(graph, file):
    graph.serialize(file)


with Flow("Wikidata Relation Enrichment") as flow:
    environment = Parameter("environment", default="production")
    endpoint = Parameter("endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    wd_endpoint = Parameter("wd_endpoint", default="https://query.wikidata.org/sparql")
    target_graph = Parameter("target_graph", default="http://intavia.eu/wikidata-relation-enrichment") # string
    target_file = Parameter("target_file", default="wikidata_personrelations.ttl")
    result_limit = Parameter("result_limit", default=None)
    intavia_sparql = setup_sparql_connection(endpoint)
    wd_sparql = setup_wd_sparql_connection(wd_endpoint)
    relation_graph = create_wikidata_connections(intavia_sparql, wd_sparql, result_limit)
    serialize_data(environment, endpoint, target_graph, relation_graph, target_file)

flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="intavia_links_from_wikidata.py")

'''
flow.run(
    environment="development",
    endpoint="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql",
    wd_endpoint="https://query.wikidata.org/sparql",
    result_limit=None, # value like 5000 for faster testing
    target_graph="http://intavia.eu/wikidata-relation-enrichment",
    target_file="../source-dataset-conversion/wikidata_personrelations/wikidata_personrelations.ttl"
)

# test in command line
# RDFDB_USER='...' RDFDB_PASSWORD='...' poetry run python3.9 intavia_links_from_wikidata.py

'''