from requests.auth import HTTPBasicAuth
import rdflib 
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import URIRef, Namespace
from rdflib.namespace import OWL, RDF
import prefect
from prefect import Flow, task, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
import requests
import os
import itertools

PREFIXES = """
PREFIX bds: <http://www.bigdata.com/rdf/search#>
PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
PREFIX bioc: <http://ldf.fi/schema/bioc/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX idm: <https://www.intavia.eu/idm/>
PREFIX idmcore: <http://www.intavia.eu/idm-core/>
PREFIX idmcores: <https://www.intavia.eu/idm-core/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
"""


def wikidata_to_gnd(sparql, wikidata_ids):
    # Wikidata endpoint doesn't handle ~30 000 inline URIs in the query, so split the query:
    res = []
    BATCH_SIZE = 15000
    for start_index in range(0, len(wikidata_ids), BATCH_SIZE):
                
        sparql.setQuery(PREFIXES + """
        SELECT * {
        VALUES ?wikidata_uri {<""" + '> <'.join(dict(itertools.islice(wikidata_ids.items(), start_index)).keys()) + """>}
        ?wikidata_uri wdt:P227 ?gnd_id .
        BIND (URI(CONCAT("https://d-nb.info/gnd/", ?gnd_id)) AS ?gnd_uri)   
        }
        #LIMIT 1
        """)

        sparql.setReturnFormat(JSON)
        sparql.setMethod('POST')
        results = sparql.query().convert()
        res += results["results"]["bindings"]

    return res

def wikidata_to_viaf(sparql, wikidata_ids):
    res = []
    BATCH_SIZE = 15000
    for start_index in range(0, len(wikidata_ids), BATCH_SIZE):
                
        sparql.setQuery(PREFIXES + """
        SELECT * {
        VALUES ?wikidata_uri {<""" + '> <'.join(dict(itertools.islice(wikidata_ids.items(), start_index)).keys()) + """>}
        ?wikidata_uri wdt:P214 ?viaf_id . 
        BIND (URI(CONCAT("https://viaf.org/viaf/", ?viaf_id)) AS ?viaf_uri)
        }
        #LIMIT 1
        """)
        results = sparql.query().convert()
        res += results["results"]["bindings"]

    return res

def gnd_to_wikidata(sparql, gnd_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?gnd_id {'""" + "' '".join(gnd_ids.keys()) + """'}
    ?wikidata_uri wdt:P227 ?gnd_id .
    }
    #LIMIT 1
    """)

    results = sparql.query().convert()
    return results["results"]["bindings"]

def gnd_to_viaf(sparql, gnd_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?gnd_id {'""" + "' '".join(gnd_ids.keys()) + """'}
    ?wikidata_uri wdt:P227 ?gnd_id ;
                    wdt:P214 ?viaf_id . 
    BIND (URI(CONCAT("https://viaf.org/viaf/", ?viaf_id)) AS ?viaf_uri)
    }
    #LIMIT 1
    """)
    results = sparql.query().convert()
    return results["results"]["bindings"]

def sbi_to_wikidata(sparql, sbi_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?sbi_id {'""" + "' '".join(sbi_ids.keys()) + """'}
    ?wikidata_uri wdt:P1254 ?sbi_id .
    }
    #LIMIT 1
    """)

    results = sparql.query().convert()
    return results["results"]["bindings"]

def sbi_to_gnd(sparql, sbi_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?sbi_id {'""" + "' '".join(sbi_ids.keys()) + """'}
    ?wikidata_uri wdt:P1254 ?sbi_id ;
                    wdt:P227 ?gnd_id . 
    BIND (URI(CONCAT("https://d-nb.info/gnd/", ?gnd_id)) AS ?gnd_uri)
    }
    #LIMIT 1
    """)

    results = sparql.query().convert()
    return results["results"]["bindings"]

def sbi_to_viaf(sparql, sbi_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?sbi_id {'""" + "' '".join(sbi_ids.keys()) + """'}
    ?wikidata_uri wdt:P1254 ?sbi_id ;
                    wdt:P214 ?viaf_id . 
    BIND (URI(CONCAT("https://viaf.org/viaf/", ?viaf_id)) AS ?viaf_uri)
    }
    #LIMIT 1
    """)

    results = sparql.query().convert()
    return results["results"]["bindings"]

def get_sameas_graph(
        res_wikidata_to_gnd, 
        res_wikidata_to_viaf, 
        res_gnd_to_wikidata, 
        res_gnd_to_viaf,
        res_sbi_to_wikidata,
        res_sbi_to_gnd,
        res_sbi_to_viaf,
        wikidata_ids,
        gnd_ids,
        sbi_ids):
    g = rdflib.Graph()

    for ob in res_wikidata_to_gnd:
        for intavia_uri in wikidata_ids[ob['wikidata_uri']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['gnd_uri']['value'])))

    for ob in res_wikidata_to_viaf:
        for intavia_uri in wikidata_ids[ob['wikidata_uri']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['viaf_uri']['value'])))

    for ob in res_gnd_to_wikidata:
        for intavia_uri in gnd_ids[ob['gnd_id']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['wikidata_uri']['value'])))

    for ob in res_gnd_to_viaf:
        for intavia_uri in gnd_ids[ob['gnd_id']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['viaf_uri']['value'])))

    for ob in res_sbi_to_wikidata:
        for intavia_uri in sbi_ids[ob['sbi_id']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['wikidata_uri']['value'])))

    for ob in res_sbi_to_gnd:
        for intavia_uri in sbi_ids[ob['sbi_id']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['gnd_uri']['value'])))

    for ob in res_sbi_to_viaf:
        for intavia_uri in sbi_ids[ob['sbi_id']['value']]:
            g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(ob['viaf_uri']['value'])))

    print(len(g))

    #g.serialize("intavia-person-id-enrichment.ttl")
    return g


@task
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)  
    sparql.setReturnFormat(JSON)
    sparql.setMethod('POST')  
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql

@task
def setup_wd_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)  
    sparql.setReturnFormat(JSON)
    sparql.setMethod('POST')  
    return sparql

@task
def create_sameas_graph(intavia_sparql, wd_sparql, target_graph):
    logger = prefect.context.get('logger')
    intavia_sparql.setQuery(PREFIXES + """
    SELECT * {
    # Existing sameAs links in Intavia data:
    #  APIS (graph: http://apis.acdh.oeaw.ac.at/data -> GND (URI ns: https://d-nb.info/gnd/)
    #  BS (graph: http://ldf.fi/nbf/data) -> Wikidata (URI ns: http://www.wikidata.org/entity/)
    #  SBI (graph: http://www.intavia.eu/sbi) -> Slovenska biografija (URI ns: https://www.slovenska-biografija.si/oseba/) 
    #  BiographyNet (graph: http://data.biographynet.nl/rdf/data) -> Wikidata (URI ns: http://www.wikidata.org/entity/)

    { GRAPH ?graph {
        # Don't include the previous result of the person id enrichment (this script)
        FILTER (STR(?graph) != \"""" + target_graph + """\")

        VALUES ?class { crm:E21_Person idmcore:Person_Proxy }
        ?s a ?class ;
            owl:sameAs ?ext_uri .
        }
    }
    UNION
    # Bionet
    { ?ext_uri <http://www.wikidata.org/prop/direct/651>/^<http://www.intavia.eu/idm-core/person_proxy_for> ?s . }
    
    # Filter out sameAs links to redundant national data sources
    FILTER (!STRSTARTS(STR(?ext_uri), "http://ldf.fi/nbf/"))
    FILTER (!STRSTARTS(STR(?ext_uri), "https://apis.acdh.oeaw.ac.at/"))

    BIND (IF (STRSTARTS(STR(?ext_uri), "https://www.slovenska-biografija.si/oseba/"), "https://www.slovenska-biografija.si/oseba/", REPLACE(STR(?ext_uri), "[^/]+$", "")) as ?ext_ns)
    BIND (IF (STRSTARTS(STR(?ext_uri), "https://www.slovenska-biografija.si/oseba/"), REPLACE(REPLACE(STR(?ext_uri), "^https://www.slovenska-biografija.si/oseba/sbi", ""), "/", ""), REPLACE(STR(?ext_uri), "^.+/", "")) AS ?ext_id)
    }
    #LIMIT 1
    """)

    results = intavia_sparql.query().convert()
    res = results["results"]["bindings"]
    logger.info(len(res))
    wikidata_ids = dict()
    gnd_ids = dict()
    sbi_ids = dict()

    for ob in res:
        if (ob['ext_ns']['value'] == "http://www.wikidata.org/entity/"):
            if ob['ext_uri']['value'] not in wikidata_ids:
                wikidata_ids[ob['ext_uri']['value']] = list()
            wikidata_ids[ob['ext_uri']['value']].append(ob['s']['value'])
        elif (ob['ext_ns']['value'] == "https://d-nb.info/gnd/"):
            if ob['ext_id']['value'] not in gnd_ids:
                gnd_ids[ob['ext_id']['value']] = list()
            gnd_ids[ob['ext_id']['value']].append(ob['s']['value'])
        elif (ob['ext_ns']['value'] == "https://www.slovenska-biografija.si/oseba/"):
            if ob['ext_id']['value'] not in sbi_ids:
                sbi_ids[ob['ext_id']['value']] = list()
            sbi_ids[ob['ext_id']['value']].append(ob['s']['value'])

    logger.info('wikidata_ids:' + str(len(wikidata_ids)))
    logger.info('gnd_ids:' + str(len(gnd_ids)))
    logger.info('sbi_ids:' + str(len(sbi_ids)))

    res_wikidata_to_gnd = wikidata_to_gnd(wd_sparql, wikidata_ids)
    res_wikidata_to_viaf = wikidata_to_viaf(wd_sparql, wikidata_ids)
    res_gnd_to_wikidata = gnd_to_wikidata(wd_sparql, gnd_ids)
    res_gnd_to_viaf = gnd_to_viaf(wd_sparql, gnd_ids)
    res_sbi_to_wikidata = sbi_to_wikidata(wd_sparql, sbi_ids)
    res_sbi_to_gnd = sbi_to_gnd(wd_sparql, sbi_ids)
    res_sbi_to_viaf = sbi_to_viaf(wd_sparql, sbi_ids)

    logger.info("res_wikidata_to_gnd:" +str(len(res_wikidata_to_gnd)))    
    logger.info("res_wikidata_to_viaf:" +str(len(res_wikidata_to_viaf)))    
    logger.info("res_gnd_to_wikidata:" +str(len(res_gnd_to_wikidata)))    
    logger.info("res_gnd_to_viaf:" +str(len(res_gnd_to_viaf)))    
    logger.info("res_sbi_to_wikidata:" +str(len(res_sbi_to_wikidata)))    
    logger.info("res_sbi_to_gnd:" +str(len(res_sbi_to_gnd)))    
    logger.info("res_sbi_to_viaf:" +str(len(res_sbi_to_viaf)))    

    g = get_sameas_graph(
        res_wikidata_to_gnd,
        res_wikidata_to_viaf,
        res_gnd_to_wikidata,
        res_gnd_to_viaf,
        res_sbi_to_wikidata,
        res_sbi_to_gnd,
        res_sbi_to_viaf,
        wikidata_ids,
        gnd_ids,
        sbi_ids
        )
    logger.info(len(g))
    return g

@task()
def update_target_graph(endpoint, target_uri, data):
    logger = prefect.context.get('logger')
    delete_url =  endpoint + '?c=<' + target_uri + '>'
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    post_url =  endpoint + '?context-uri=' + target_uri + ''
    res = requests.delete(delete_url, auth=auth)
    res2 = requests.post(post_url, headers={'Content-type': 'text/turtle'}, data=data.serialize(), auth=auth)
    logger.info(res)
    logger.info(res2)


with Flow("Person ID Linker") as flow:
    endpoint = Parameter("endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    wd_endpoint = Parameter("wd_endpoint", default="https://query.wikidata.org/sparql") 
    target_graph = Parameter("target_graph", default="http://www.intavia.org/graphs/person-id-enrichment") # string
    intavia_sparql = setup_sparql_connection(endpoint)
    wd_sparql = setup_wd_sparql_connection(wd_endpoint)
    print(intavia_sparql)
    sameas_graph = create_sameas_graph(intavia_sparql, wd_sparql, target_graph)
    update_target_graph(endpoint, target_graph, sameas_graph)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="person_id_linker.py")
