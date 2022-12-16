import datetime
from zoneinfo import ZoneInfo
import prefect
from prefect import task
import uuid
import rdflib
from rdflib.namespace import RDF, RDFS, XSD
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
import urllib
import os
from requests.auth import HTTPBasicAuth
import requests
from enum import Enum

IDMCORE = rdflib.Namespace("http://www.intavia.eu/idm-core/")
IDM_PROV = rdflib.Namespace("http://www.intavia.eu/idm-prov/")
IDM_PREFECT = rdflib.Namespace("http://www.intavia.eu/idm-prefect/")
PROV = rdflib.Namespace("http://www.w3.org/ns/prov#")
PROV_TARGET_GRAPH = 'http://www.intavia.eu/graphs/provenance'

class EntityPrefixes(str, Enum):
    SOURCE = 'source'
    TARGET = 'target'

def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql

def get_graph_id_from_source(source_uri):
    if '/version/' in source_uri:
        return source_uri[0:source_uri.index('/version/')]
    else:
        return None

def query_latest_set_id(sparql):  
    logger = prefect.context.get('logger')
    query = """
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    select ?set
    from <http://www.intavia.eu/graphs/provenance>
    where {
        ?set a <http://www.intavia.eu/idm-core/GraphSetInTime> .
        ?set <http://www.intavia.eu/idm-core/start> ?period_start . 
        MINUS {
            ?set <http://www.intavia.eu/idm-core/end> ?period_end
        }
    }    
    """
    sparql.setQuery(query)
    results = sparql.query().convert()
    bindings = results["results"]["bindings"]
    if len(bindings) == 0:
        return None

    
    for b in bindings:
        return b['set']['value']

def add_entities(g, endpoint, activityURI, start_datetime, entity_prefix, sources): 
    
    flow_run_id = prefect.context.flow_run_id    
    for index, obj in enumerate(sources):
        label = obj['label']
        source_entity = rdflib.URIRef(IDM_PROV[f'{flow_run_id}/{entity_prefix}/{index}'])
        g.add( (source_entity, RDF.type, PROV.Entity) )
        g.add( (source_entity, RDFS.label, rdflib.Literal(label)) )

        source_uri = obj['uri']
        graph_id = get_graph_id_from_source(source_uri)
        if graph_id != None:
            g.add( (source_entity, IDM_PROV.graphID, rdflib.URIRef(graph_id)))
        g.add( (source_entity, IDM_PROV.source, rdflib.URIRef(source_uri)))        
        
        if entity_prefix == EntityPrefixes.SOURCE:
            g.add( (activityURI, PROV.used, source_entity))
        else:
            g.add( (activityURI, PROV.generated, source_entity))
        
    return g

def add_end_timestamp_to_previous_set(sparql, setURI, end_datetime):
    update = """
    INSERT DATA {
        GRAPH <http://www.intavia.eu/graphs/provenance> {
        <""" + setURI + """> <http://www.intavia.eu/idm-core/end> \"""" + end_datetime.isoformat() + """\"^^<http://www.w3.org/2001/XMLSchema#dateTime>  .
        }
    }
    """
    sparql.setQuery(update)
    sparql.setMethod(POST)
    results = sparql.query()
    sparql.setMethod(GET)


def add_new_graphset(g, sparql, old_set_uri, start_datetime, targets):

    setURI = rdflib.URIRef('http://www.intavia.eu/sets/' + str(uuid.uuid4()))
    g.add( (setURI, RDF.type, rdflib.URIRef('http://www.intavia.eu/idm-core/GraphSetInTime')) )
    g.add( (setURI, rdflib.URIRef('http://www.intavia.eu/idm-core/start'), rdflib.Literal(start_datetime.isoformat(), datatype=XSD.dateTime) ) )


    
    if old_set_uri:
        # query for named graphs of the source set 
        query = """
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        select ?entity ?graphID ?source
        from <http://www.intavia.eu/graphs/provenance>
        where {
            <""" + old_set_uri + """> <http://www.intavia.eu/idm-core/namedGraphs> ?entity .
            ?entity <http://www.intavia.eu/idm-prov/graphID> ?graphID .
            ?entity <http://www.intavia.eu/idm-prov/source> ?source 
        }        
        """
        sparql.setQuery(query)
        
        graphIDs = list(map(lambda _target: get_graph_id_from_source(_target['uri']), targets))
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:    
            graphID = result['graphID']['value']
            entity = result['entity']['value']
            source = result['entity']['source']
            if graphID not in graphIDs:
                g.add( (setURI, rdflib.URIRef('http://www.intavia.eu/idm-core/namedGraphs'), rdflib.URIRef(source)) )
    
    for target in targets:
        uri = target['uri']
        g.add( (setURI, rdflib.URIRef('http://www.intavia.eu/idm-core/namedGraphs'), rdflib.URIRef(uri) ) )
    return g


def get_named_graphs_for_graph_ids(endpoint, timestamp, graph_ids):
    """
        Returns the named graph for a graphID at a certain point in time.
        If timestamp is None, the graph URI with the latest data will be returned.
        If no graph is found, None will be returned. This can happen if no datasets 
        (i.e. named graphs) were created for the given graphID.

        Returns a dictionary with graph_id as the key and versioned graph URI as the value.
    """          
    logger = prefect.context.get('logger')
    graphIDFilter = ','.join(list(map(lambda x: '<' + x + '>', graph_ids)))
    if timestamp:
        timestamp_filter = 'FILTER(?period_start < "' + timestamp.isoformat() + '"^^xsd:dateTime )'
    else:
        timestamp_filter = 'MINUS { ?set <http://www.intavia.eu/idm-core/end> ?period_end }'
    query = """
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    select ?uri ?set ?graphID
    from <http://www.intavia.eu/graphs/provenance>
    where {
        ?set a <http://www.intavia.eu/idm-core/GraphSetInTime> .
        ?set <http://www.intavia.eu/idm-core/start> ?period_start . 
        """ + timestamp_filter + """
        ?set <http://www.intavia.eu/idm-core/namedGraphs> ?uri .
        ?entity <http://www.intavia.eu/idm-prov/graphID> ?graphID .
        ?entity <http://www.intavia.eu/idm-prov/source> ?uri
        FILTER(?graphID in (""" + graphIDFilter + """))
    }    
    """

    sparql = setup_sparql_connection(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    bindings = results["results"]["bindings"]
    if len(bindings) == 0:
        return None

    results = {}
    for b in bindings:
        results[b['graphID']['value']] = b['uri']['value']
        
    return results

def get_start_time():
    schedule_time = prefect.context.get("scheduled_start_time")
    if schedule_time:
        return schedule_time
    else:
        datetime.datetime.now(ZoneInfo('GMT'))

@task
def generate_versioned_named_graph(graph_id):
    return graph_id + '/version/' + str(uuid.uuid4())

@task
def add_provenance_data(_, endpoint, sources, targets):
    start_time =  get_start_time()
    sparql = setup_sparql_connection(endpoint)

    logger = prefect.context.get('logger')    
    flow_name = prefect.context.flow_name
    flow_id = prefect.context.flow_id
    flow_run_id = prefect.context.get('flow_run_id', 'not-available')
    flow_run_version = prefect.context.get('flow_run_version', 'not-available')    
    end_time = datetime.datetime.now(ZoneInfo('GMT'))

    g = rdflib.Graph()

    # workflow
    workflowURI = rdflib.URIRef(IDM_PROV['workflow/'] + urllib.parse.quote(flow_id))
    g.add( (workflowURI, RDFS.label, rdflib.Literal(flow_name)))

    # prov activity
    #activityURI = rdflib.URIRef(IDM_PROV['activity/' + str(uuid.uuid4())])
    activityURI = rdflib.URIRef(IDM_PROV['activity/' + flow_run_id])
    g.add((activityURI, RDF.type, PROV.Activity))
    g.add((activityURI, IDM_PREFECT.flow_name, rdflib.Literal(flow_name)))
    g.add((activityURI, IDM_PREFECT.flow_id, rdflib.Literal(flow_id)))
    g.add((activityURI, IDM_PREFECT.flow_run_version, rdflib.Literal(flow_run_version)))
    g.add((activityURI, PROV.startedAtTime, rdflib.Literal(start_time.isoformat(), datatype=XSD.dateTime)))
    g.add((activityURI, PROV.endedAtTime, rdflib.Literal(end_time.isoformat(), datatype=XSD.dateTime)))
    g.add((activityURI, IDM_PROV.workflow, workflowURI))


    # used & generated
    add_entities(g, endpoint, activityURI, start_time, EntityPrefixes.SOURCE, sources)
    add_entities(g, endpoint, activityURI, None, EntityPrefixes.TARGET, targets)

    # graph sets
    old_set_uri = query_latest_set_id(sparql)
    add_new_graphset(g, sparql, old_set_uri, end_time, targets)
    if old_set_uri:
        add_end_timestamp_to_previous_set(sparql, old_set_uri, end_time)

    logger.info(g.serialize())
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    post_url =  endpoint + '?context-uri=' + PROV_TARGET_GRAPH + ''
    res2 = requests.post(post_url, headers={'Content-type': 'text/turtle'}, data=g.serialize(), auth=auth)
    logger.info(res2)