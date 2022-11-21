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
        end_index = start_index+BATCH_SIZE-1
        if end_index > len(wikidata_ids):
            end_index = len(wikidata_ids)

        sparql.setQuery(PREFIXES + """
        SELECT * {
        VALUES ?wikidata_uri {<""" + '> <'.join(dict(itertools.islice(wikidata_ids.items(), start_index, end_index)).keys()) + """>}
        ?wikidata_uri wdt:P227 ?gnd_id .
        BIND (URI(CONCAT("https://d-nb.info/gnd/", ?gnd_id)) AS ?gnd_uri)   
        }
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
        end_index = start_index+BATCH_SIZE-1
        if end_index > len(wikidata_ids):
            end_index = len(wikidata_ids)

        sparql.setQuery(PREFIXES + """
        SELECT * {
        VALUES ?wikidata_uri {<""" + '> <'.join(dict(itertools.islice(wikidata_ids.items(), start_index, end_index)).keys()) + """>}
        ?wikidata_uri wdt:P214 ?viaf_id . 
        BIND (URI(CONCAT("https://viaf.org/viaf/", ?viaf_id)) AS ?viaf_uri)
        }
        """)
        results = sparql.query().convert()
        res += results["results"]["bindings"]

    return res

def wikidata_to_geonames(sparql, wikidata_ids):
    res = []
    BATCH_SIZE = 15000
    for start_index in range(0, len(wikidata_ids), BATCH_SIZE):
        end_index = start_index+BATCH_SIZE-1
        if end_index > len(wikidata_ids):
            end_index = len(wikidata_ids)

        sparql.setQuery(PREFIXES + """
        SELECT * {
        VALUES ?wikidata_uri {<""" + '> <'.join(dict(itertools.islice(wikidata_ids.items(), start_index, end_index)).keys()) + """>}
        ?wikidata_uri wdt:P1566 ?geonames_id .
        BIND (URI(CONCAT("https://sws.geonames.org/", ?geonames_id, "/")) AS ?geonames_uri)
        }
        """)
        results = sparql.query().convert()
        res += results["results"]["bindings"]

    return res

def gnd_to_wikidata(sparql, gnd_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?gnd_id {'""" + "' '".join(gnd_ids.keys()) + """'}
    ?wikidata_uri wdt:P227 ?gnd_id .
    BIND (URI(CONCAT("https://d-nb.info/gnd/", ?gnd_id)) AS ?gnd_uri)
    }
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
    BIND (URI(CONCAT("https://d-nb.info/gnd/", ?gnd_id)) AS ?gnd_uri)
    }
    """)
    results = sparql.query().convert()
    return results["results"]["bindings"]

def geonames_to_wikidata(sparql, gnd_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?geonames_id {'""" + "' '".join(gnd_ids.keys()) + """'}
    ?wikidata_uri wdt:P1566 ?geonames_id .
    BIND (URI(CONCAT("https://sws.geonames.org/", ?geonames_id, "/")) AS ?geonames_uri)
    }
    """)

    results = sparql.query().convert()
    return results["results"]["bindings"]

def geonames_to_viaf(sparql, gnd_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?geonames_id {'""" + "' '".join(gnd_ids.keys()) + """'}
    ?wikidata_uri wdt:P1566 ?geonames_id ;
                  wdt:P214 ?viaf_id .
    BIND (URI(CONCAT("https://viaf.org/viaf/", ?viaf_id)) AS ?viaf_uri)
    BIND (URI(CONCAT("https://sws.geonames.org/", ?geonames_id, "/")) AS ?geonames_uri)
    }
    """)
    results = sparql.query().convert()
    return results["results"]["bindings"]

def sbi_to_wikidata(sparql, sbi_ids):
    sparql.setQuery(PREFIXES + """
    SELECT * {
    VALUES ?sbi_id {'""" + "' '".join(sbi_ids.keys()) + """'}
    ?wikidata_uri wdt:P1254 ?sbi_id .
    BIND (URI(CONCAT("https://www.slovenska-biografija.si/oseba/sbi", ?sbi_id, "/")) AS ?sbi_uri)
    }
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
    BIND (URI(CONCAT("https://www.slovenska-biografija.si/oseba/sbi", ?sbi_id, "/")) AS ?sbi_uri)
    }
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
    BIND (URI(CONCAT("https://www.slovenska-biografija.si/oseba/sbi", ?sbi_id, "/")) AS ?sbi_uri)
    }
    """)

    results = sparql.query().convert()
    return results["results"]["bindings"]

def bp_to_wikidata(sparql, bp_ids):
    res = []
    BATCH_SIZE = 15000
    for start_index in range(0, len(bp_ids), BATCH_SIZE):
        end_index = start_index+BATCH_SIZE-1
        if end_index > len(bp_ids):
            end_index = len(bp_ids)

        sparql.setQuery(PREFIXES + """
        SELECT * {
        VALUES ?bp_id {'""" + "' '".join(dict(itertools.islice(bp_ids.items(), start_index, end_index)).keys()) + """'}
        ?wikidata_uri wdt:P651 ?bp_id .
        }
        """)

        results = sparql.query().convert()
        res += results["results"]["bindings"]

    return res

def get_ext_uris_for_wikidata(res, wikidata_uri_fieldname, other_uri_fieldname, wikidata_to_others):
    for ob in res:
        if ob[wikidata_uri_fieldname]['value'] not in wikidata_to_others:
           wikidata_to_others[ob[wikidata_uri_fieldname]['value']] = list()
        wikidata_to_others[ob[wikidata_uri_fieldname]['value']].append(ob[other_uri_fieldname]['value'])
    #return wikidata_to_others

def get_sameas_graph(
        res_wikidata_to_gnd,
        res_wikidata_to_viaf,
        res_wikidata_to_geonames,
        res_gnd_to_wikidata,
        res_gnd_to_viaf,
        res_geonames_to_wikidata,
        res_geonames_to_viaf,
        res_sbi_to_wikidata,
        res_sbi_to_gnd,
        res_sbi_to_viaf,
        wikidata_ids,
        entity_proxy_uri_prefix,
        entity_proxy_class):
    g = rdflib.Graph()

    wikidata_to_others = dict()
    get_ext_uris_for_wikidata(res_wikidata_to_gnd, "wikidata_uri", "gnd_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_wikidata_to_viaf, "wikidata_uri", "viaf_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_wikidata_to_geonames, "wikidata_uri", "geonames_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_gnd_to_wikidata, "wikidata_uri", "gnd_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_gnd_to_viaf, "wikidata_uri", "gnd_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_gnd_to_viaf, "wikidata_uri", "viaf_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_geonames_to_wikidata, "wikidata_uri", "geonames_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_geonames_to_viaf, "wikidata_uri", "geonames_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_geonames_to_viaf, "wikidata_uri", "viaf_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_sbi_to_wikidata, "wikidata_uri", "sbi_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_sbi_to_gnd, "wikidata_uri", "sbi_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_sbi_to_gnd, "wikidata_uri", "gnd_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_sbi_to_viaf, "wikidata_uri", "sbi_uri", wikidata_to_others)
    get_ext_uris_for_wikidata(res_sbi_to_viaf, "wikidata_uri", "viaf_uri", wikidata_to_others)

    entity_proxy_local_id_counter = 1

    for wikidata_uri, other_uris in wikidata_to_others.items():
        g.add((URIRef(entity_proxy_uri_prefix+str(entity_proxy_local_id_counter)), RDF.type, URIRef(entity_proxy_class)))
        g.add((URIRef(entity_proxy_uri_prefix+str(entity_proxy_local_id_counter)), OWL.sameAs, URIRef(wikidata_uri)))
        for other_uri in other_uris:
            g.add((URIRef(entity_proxy_uri_prefix+str(entity_proxy_local_id_counter)), OWL.sameAs, URIRef(other_uri)))
        entity_proxy_local_id_counter += 1

    for wikidata_uri in wikidata_ids:
        if wikidata_uri not in wikidata_to_others: # others are already added
            g.add((URIRef(entity_proxy_uri_prefix+str(entity_proxy_local_id_counter)), RDF.type, URIRef(entity_proxy_class)))
            g.add((URIRef(entity_proxy_uri_prefix+str(entity_proxy_local_id_counter)), OWL.sameAs, URIRef(wikidata_uri)))
            entity_proxy_local_id_counter += 1

    # add Bionet -> Wikidata mappings to Bionet person proxies as they are not included in the source data
    if entity_proxy_class == "http://www.intavia.eu/idm-core/Person_Proxy": # hack
        for wikidata_id, intavia_uris in wikidata_ids.items():
            for intavia_uri in intavia_uris:
                if intavia_uri.startswith("http://data.biographynet.nl/rdf/"):
                    g.add((URIRef(intavia_uri), OWL.sameAs, URIRef(wikidata_id)))

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
def create_sameas_graph(intavia_sparql, wd_sparql, target_graph, entity_proxy_uri_prefix, entity_proxy_class):

    logger = prefect.context.get('logger')

    # hack
    bionet_query_part = ""
    if (entity_proxy_class == "http://www.intavia.eu/idm-core/Person_Proxy"):
        bionet_query_part = """
        UNION
        { ?provided_person <http://data.biographynet.nl/rdf/personID> ?ext_id .
        BIND ("http://www.biografischportaal.nl/" AS ?ext_ns)
        ?entity_proxy idmcore:person_proxy_for ?provided_person .
        }
        """

    # hack
    entity_proxy_class_for_query = entity_proxy_class
    if entity_proxy_class == "http://www.intavia.eu/idm-core/Place_Proxy":
        entity_proxy_class_for_query = "http://www.cidoc-crm.org/cidoc-crm/E53_Place"

    intavia_sparql.setQuery(PREFIXES + """
    SELECT * {
    # Existing person sameAs links in Intavia data:
    #  APIS (graph: http://apis.acdh.oeaw.ac.at/data -> GND (URI ns: https://d-nb.info/gnd/)
    #  BS (graph: http://ldf.fi/nbf/data) -> Wikidata (URI ns: http://www.wikidata.org/entity/)
    #  SBI (graph: http://www.intavia.eu/sbi) -> Slovenska biografija (URI ns: https://www.slovenska-biografija.si/oseba/) 
    # Wikidata -> Dutch Biography Portal
    ?entity_proxy a <""" + entity_proxy_class_for_query + """> .
    {
      GRAPH ?graph {
        # Don't include the previous result of the person id enrichment (this script)
        FILTER (STR(?graph) != \"""" + target_graph + """\")

        ?entity_proxy # a <""" + entity_proxy_class_for_query + """> ;
                      owl:sameAs ?ext_uri .
      }
    }
    """ + bionet_query_part + """
    BIND (IF (STRSTARTS(STR(?ext_uri), "https://www.slovenska-biografija.si/oseba/"), "https://www.slovenska-biografija.si/oseba/", IF (STRSTARTS(STR(?ext_uri), "https://sws.geonames.org/"), "https://sws.geonames.org/", REPLACE(STR(?ext_uri), "[^/]+$", ""))) as ?ext_ns)
    BIND (IF (STRSTARTS(STR(?ext_uri), "https://www.slovenska-biografija.si/oseba/"), REPLACE(REPLACE(STR(?ext_uri), "^https://www.slovenska-biografija.si/oseba/sbi", ""), "/", ""), IF (STRSTARTS(STR(?ext_uri), "https://sws.geonames.org/"),  REPLACE(REPLACE(STR(?ext_uri), "^https://sws.geonames.org/", ""), "/", ""), REPLACE(STR(?ext_uri), "^.+/", ""))) AS ?ext_id)
    }
    """)

    results = intavia_sparql.query().convert()
    res = results["results"]["bindings"]
    logger.info(len(res))
    wikidata_ids = dict()
    gnd_ids = dict()
    geonames_ids = dict()
    sbi_ids = dict()
    bp_ids = dict() # Dutch Biography Portal

    for ob in res:
        if (ob['ext_ns']['value'] == "http://www.wikidata.org/entity/"):
            if ob['ext_uri']['value'] not in wikidata_ids:
                wikidata_ids[ob['ext_uri']['value']] = list()
            wikidata_ids[ob['ext_uri']['value']].append(ob['entity_proxy']['value'])
        elif (ob['ext_ns']['value'] == "https://d-nb.info/gnd/"):
            if ob['ext_id']['value'] not in gnd_ids:
                gnd_ids[ob['ext_id']['value']] = list()
            gnd_ids[ob['ext_id']['value']].append(ob['entity_proxy']['value'])
        elif (ob['ext_ns']['value'] == "https://sws.geonames.org/"):
            if ob['ext_id']['value'] not in geonames_ids:
                geonames_ids[ob['ext_id']['value']] = list()
            geonames_ids[ob['ext_id']['value']].append(ob['entity_proxy']['value'])
        elif (ob['ext_ns']['value'] == "https://www.slovenska-biografija.si/oseba/"):
            if ob['ext_id']['value'] not in sbi_ids:
                sbi_ids[ob['ext_id']['value']] = list()
            sbi_ids[ob['ext_id']['value']].append(ob['entity_proxy']['value'])
        elif (ob['ext_ns']['value'] == "http://www.biografischportaal.nl/"):
            if ob['ext_id']['value'] not in bp_ids:
                bp_ids[ob['ext_id']['value']] = list()
            bp_ids[ob['ext_id']['value']].append(ob['entity_proxy']['value'])
    logger.info('wikidata_ids:' + str(len(wikidata_ids)))
    logger.info('gnd_ids:' + str(len(gnd_ids)))
    logger.info('geonames_ids:' + str(len(geonames_ids)))
    logger.info('sbi_ids:' + str(len(sbi_ids)))
    logger.info('bp_ids:' + str(len(bp_ids)))

    res_bp_to_wikidata = bp_to_wikidata(wd_sparql, bp_ids)
    for ob in res_bp_to_wikidata:
        if ob['wikidata_uri']['value'] not in wikidata_ids:
            wikidata_ids[ob['wikidata_uri']['value']] = list()
        wikidata_ids[ob['wikidata_uri']['value']].extend(bp_ids[ob['bp_id']['value']])

    res_wikidata_to_gnd = wikidata_to_gnd(wd_sparql, wikidata_ids)
    res_wikidata_to_viaf = wikidata_to_viaf(wd_sparql, wikidata_ids)
    res_wikidata_to_geonames = wikidata_to_geonames(wd_sparql, wikidata_ids)
    res_gnd_to_wikidata = gnd_to_wikidata(wd_sparql, gnd_ids)
    res_gnd_to_viaf = gnd_to_viaf(wd_sparql, gnd_ids)
    res_geonames_to_wikidata = geonames_to_wikidata(wd_sparql, geonames_ids)
    res_geonames_to_viaf = geonames_to_viaf(wd_sparql, geonames_ids)
    res_sbi_to_wikidata = sbi_to_wikidata(wd_sparql, sbi_ids)
    res_sbi_to_gnd = sbi_to_gnd(wd_sparql, sbi_ids)
    res_sbi_to_viaf = sbi_to_viaf(wd_sparql, sbi_ids)

    logger.info("res_wikidata_to_gnd:" +str(len(res_wikidata_to_gnd)))
    logger.info("res_wikidata_to_viaf:" +str(len(res_wikidata_to_viaf)))
    logger.info("res_wikidata_to_geonames:" +str(len(res_wikidata_to_geonames)))
    logger.info("res_gnd_to_wikidata:" +str(len(res_gnd_to_wikidata)))
    logger.info("res_gnd_to_viaf:" +str(len(res_gnd_to_viaf)))
    logger.info("res_geonames_to_wikidata:" +str(len(res_geonames_to_wikidata)))
    logger.info("res_geonames_to_viaf:" +str(len(res_geonames_to_viaf)))
    logger.info("res_sbi_to_wikidata:" +str(len(res_sbi_to_wikidata)))
    logger.info("res_sbi_to_gnd:" +str(len(res_sbi_to_gnd)))
    logger.info("res_sbi_to_viaf:" +str(len(res_sbi_to_viaf)))

    g = get_sameas_graph(
        res_wikidata_to_gnd,
        res_wikidata_to_viaf,
        res_wikidata_to_geonames,
        res_gnd_to_wikidata,
        res_gnd_to_viaf,
        res_geonames_to_wikidata,
        res_geonames_to_viaf,
        res_sbi_to_wikidata,
        res_sbi_to_gnd,
        res_sbi_to_viaf,
        wikidata_ids,
        entity_proxy_uri_prefix,
        entity_proxy_class)
    logger.info(len(g))
    return g

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

@task()
def serialize_data(environment, endpoint, target_uri, data, file):
    if environment == "production":
        update_target_graph(endpoint, target_uri, data)
    else:
        write_graph_to_file(data, file) # for local dev

with Flow("Entity ID Linker") as flow:
    environment = Parameter("environment", default="production")
    endpoint = Parameter("endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    wd_endpoint = Parameter("wd_endpoint", default="https://query.wikidata.org/sparql") 
    target_graph = Parameter("target_graph", default="http://www.intavia.org/graphs/person-id-enrichment") # string
    entity_proxy_uri_prefix = Parameter("entity_proxy_uri_prefix", default="http://www.intavia.eu/entity-id-linker/person_proxy_")
    entity_proxy_class = Parameter("entity_proxy_class", default="http://www.intavia.eu/idm-core/Person_Proxy")
    target_file = Parameter("target_file", default="intavia-person-id-enrichment.ttl")
    intavia_sparql = setup_sparql_connection(endpoint)
    wd_sparql = setup_wd_sparql_connection(wd_endpoint)
    sameas_graph = create_sameas_graph(intavia_sparql, wd_sparql, target_graph, entity_proxy_uri_prefix, entity_proxy_class)
    serialize_data(environment, endpoint, target_graph, sameas_graph, target_file)

flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="entity_id_linker.py")

# Persons
#flow.run(
#    environment="development",
#    endpoint="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql",
#    wd_endpoint="https://query.wikidata.org/sparql",
#    target_graph="http://www.intavia.org/graphs/person-id-enrichment",
#    entity_proxy_uri_prefix="http://www.intavia.eu/entity-id-linker/person_proxy_",
#    entity_proxy_class="http://www.intavia.eu/idm-core/Person_Proxy",
#    target_file="intavia-person-id-enrichment.ttl" # parameter not needed for production
#)

# Places
#flow.run(
#    environment="development",
#    endpoint="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql",
#    wd_endpoint="https://query.wikidata.org/sparql",
#    target_graph="http://www.intavia.org/graphs/place-id-enrichment",
#    entity_proxy_uri_prefix="http://www.intavia.eu/entity-id-linker/place_proxy_",
#    entity_proxy_class="http://www.intavia.eu/idm-core/Place_Proxy",
#    target_file="intavia-place-id-enrichment.ttl" # parameter not needed for production
#)
