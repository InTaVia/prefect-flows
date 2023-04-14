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

def ulanurlquery(urls = [], limit = None):
  '''Query Getty-Ulan URLS via Wikidata endpoint'''
  return """PREFIX bd: <http://www.bigdata.com/rdf#>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX schema: <http://schema.org/>
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  prefix wds: <http://www.wikidata.org/entity/statement/>
  PREFIX wdt: <http://www.wikidata.org/prop/direct/>
  PREFIX wd: <http://www.wikidata.org/entity/>
  PREFIX wikibase: <http://wikiba.se/ontology#> 

  SELECT DISTINCT ?wiki (CONCAT('http://vocab.getty.edu/ulan/', str(?_ulan)) AS ?ulan)
  WHERE {
    VALUES ?wiki { """ + ' '.join((f'<{s}>' for s in urls)) + """ }
    ?wiki wdt:P245 ?_ulan
    }""" + (f'LIMIT {limit}' if limit else '')

def gettylinkquery(urls = [], limit = None):
  '''Query relations between people. 
  Query example in yasgui: https://api.triplydb.com/s/G2uTtjzHW
  '''
  return """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  prefix gvp: <http://vocab.getty.edu/ontology#>
  prefix ulan: <http://vocab.getty.edu/ulan/>
  prefix xl: <http://www.w3.org/2008/05/skos-xl#>
  prefix foaf: <http://xmlns.com/foaf/0.1/>
  prefix schema: <http://schema.org/>

  SeLECT DISTINcT ?actor1 ?actor1Label ?relation ?prop ?actor2 ?actor2Label
  {
    VALUES ?actor1 { """ + ' '.join((f'<{s}>' for s in urls)) + """ }

  VALUES (?prop ?relation) {
    (gvp:ulan1101_teacher_of "student")
    (gvp:ulan1102_student_of "teacher")
    (gvp:ulan1303_collaborated_with "collaborator")
    (gvp:ulan1105_apprentice_of "master")
    (gvp:ulan1106_apprentice_was "apprentice")
    (gvp:ulan1305_worked_with "coworker")
    (gvp:ulan1218_employee_was "employer")
    (gvp:ulan1217_employee_of "employee")
    (gvp:ulan1202_patron_was "patron")
    (gvp:ulan1201_patron_of "patron_of")
    (gvp:ulan1308_assisted_by "assistant")
    (gvp:ulan1311_partner_of "partner")
    (gvp:ulan1543_consort_was "consort")
    (gvp:ulan1542_consort_of "consort_of")
    (gvp:ulan2550_friend_of "friend")
    (gvp:ulan1301_colleague_of "colleague")
    (gvp:ulan1107_influenced "influenced")
    (gvp:ulan1108_influenced_by "influencer") 
  }

  ?actor1 ?prop ?actor2 .
  
  ?actor1 a gvp:PersonConcept ; gvp:prefLabelGVP [xl:literalForm ?actor1Label] . 

  ?actor2 gvp:prefLabelGVP [xl:literalForm ?actor2Label] ; a gvp:PersonConcept .
} """ + (f'LIMIT {limit}' if limit else '')

def queryDatabase(wrapper, urls, query,  wikilimit = 10000, limit = 20000):
  if len(urls)>10000:
    # split query into smaller chunks
    return queryDatabase(wrapper, urls[:wikilimit], query, wikilimit = wikilimit, limit=limit) + queryDatabase(wrapper, urls[wikilimit:], query, wikilimit = wikilimit, limit=limit)
  
  wrapper.setQuery(query(urls = urls, limit = limit))

  results = wrapper.query().convert()

  return convertDatatypes(results)

def getGettydata(sparql, URLS):
  return queryDatabase(sparql, URLS, gettylinkquery, wikilimit = 10000)

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
def setup_getty_sparql_connection(endpoint):
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
def create_getty_connections(intavia_sparql, wd_sparql, getty_sparql, limit):
  logger = prefect.context.get('logger')
  # logger.debug = print
  
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

  ulans = queryDatabase(wd_sparql, URLS, ulanurlquery, wikilimit = 10000)
  logger.debug(f"All {len(ulans)} results")
  for ob in ulans[:10]:
     logger.debug(ob)

  ULAN_URLS = [ob.get('ulan') for ob in ulans]

  links = getGettydata(getty_sparql, ULAN_URLS)

  # choose links where to target is also in InTaVia
  links = [ob for ob in links if ob.get('actor2') in ULAN_URLS] 
  logger.debug(f"Filtered to {len(links)} results in InTaVia")

  LOOKUP_BY_GETTYURL = dict((ob.get('ulan'), LOOKUP_BY_WIKIURL.get(ob.get('wiki'))) for ob in ulans)

  for ob in ulans[:10]:
     u = ob.get('ulan')
     logger.debug(u, LOOKUP_BY_GETTYURL.get(u, '-'))
  
  '''
  31 Mar 2023, added family relations
  Altogether 46434 results
  Filtered to 18325 results in InTaVia
  '''

  """## add InTaVia URLs and data sources"""

  for ob in links:
    ob['url1'] = LOOKUP_BY_GETTYURL.get(ob.get('actor1'))
    ob['url2'] = LOOKUP_BY_GETTYURL.get(ob.get('actor2'))

  links.sort(key = lambda ob: (ob.get('url1'), ob.get('relation'), ob.get('url2')))

  BIOC = Namespace('http://ldf.fi/schema/bioc/')
  INTAVIA_RELATIONS = Namespace('http://www.intavia.eu/getty/personrelation/')
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
    getty_endpoint = Parameter("getty_endpoint", default="http://vocab.getty.edu/sparql")
    target_graph = Parameter("target_graph", default="http://intavia.eu/ulan-relation-enrichment") # string
    target_file = Parameter("target_file", default="ulan_personrelations.ttl")
    result_limit = Parameter("result_limit", default=None)
    intavia_sparql = setup_sparql_connection(endpoint)
    wd_sparql = setup_getty_sparql_connection(wd_endpoint)
    getty_sparql = setup_getty_sparql_connection(getty_endpoint)
    relation_graph = create_getty_connections(intavia_sparql, wd_sparql, getty_sparql, result_limit)
    serialize_data(environment, endpoint, target_graph, relation_graph, target_file)

flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="intavia_links_from_ulan.py")

'''
flow.run(
    environment="development",
    endpoint="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql",
    getty_endpoint="http://vocab.getty.edu/sparql",
    result_limit=None, # value like 5000 for faster testing
    target_graph="http://intavia.eu/ulan-relation-enrichment",
    target_file="../source-dataset-conversion/ulan_personrelations/ulan_personrelations.ttl"
)

# test in command line
# RDFDB_USER='...' RDFDB_PASSWORD='...' poetry run python3.9 intavia_links_from_ulan.py

'''