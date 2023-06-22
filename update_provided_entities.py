from venv import create
from SPARQLWrapper import SPARQLWrapper, JSON
from requests.auth import HTTPBasicAuth
from rdflib import URIRef, Namespace, Graph, Literal, XSD
from rdflib.namespace import OWL, RDF, RDFS
import prefect
from prefect import Flow, task, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
import requests
import os
from string import Template
import datetime

IDM_PROV = Namespace("http://www.intavia.eu/idm-prov/")
IDM_PREFECT = Namespace("http://www.intavia.eu/idm-prefect/")
PROV = Namespace("http://www.w3.org/ns/prov#")
PROV_TARGET_GRAPH = 'http://www.intavia.org/graphs/provenance'

@task(log_stdout=True)
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql


@task
def get_sameas_statements(sparql, entity_source_uris, entity_source_type, entity_source_proxy_type):
    logger = prefect.context.get('logger')

    from_part = '\n'.join(
        list(map(lambda x: 'from <' + x + '>', entity_source_uris)))
    loadQuery = """  
  PREFIX owl: <http://www.w3.org/2002/07/owl#> 
  CONSTRUCT {
    ?s owl:sameAs ?o
  }
  """ + from_part + """
  WHERE {
    VALUES ?class {<""" + entity_source_type + """> <""" + entity_source_proxy_type + """>}
    ?s owl:sameAs ?o ;
       a ?class .
  }
  """

    sparql.setQuery(loadQuery)
    results = sparql.queryAndConvert()

    with open('/tmp/lg_data.ttl', 'w') as file:
        file.write(results.serialize())

    g = Graph()
    g.parse('/tmp/lg_data.ttl')

    logger.info('Linking data loaded')
    logger.info('sameAs statements: ' + str(len(results)))
    return g


@task
def create_provided_entities_graph(sparql, id_graph, entity_enriched_uris, entity_source_type, entity_source_proxy_type, provided_entity_ns, provided_entity_type, entity_proxy_for_property):
    logger = prefect.context.get('logger')

    entityQuery = """
  SELECT DISTINCT ?entity
  FROM <""" + entity_enriched_uris + """>
  WHERE
  {
    VALUES ?class {<""" + entity_source_type + """> <""" + entity_source_proxy_type + """>}
    ?entity a ?class
  } 
  """

    providedEntityTypeURI = URIRef(provided_entity_type)
    entityProxyForPropertyURI = URIRef(entity_proxy_for_property)

    g = Graph()

    addedEntityProxies = set()

    providedEntityCount = 0
    sparql.setQuery(entityQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    for result in results["results"]["bindings"]:
        entityProxyURI = result['entity']['value']
        check = False
        entityProxy = URIRef(entityProxyURI)
        providedEntityURI = URIRef(
            provided_entity_ns + str(providedEntityCount))
        g.add((providedEntityURI, RDF.type, providedEntityTypeURI))
        # check for owl:sameAs links
        added = False
        for extID in id_graph.objects(entityProxy, OWL.sameAs):
            for otherProxy in id_graph.subjects(OWL.sameAs, extID):
                if not otherProxy in addedEntityProxies:
                    g.add((otherProxy, entityProxyForPropertyURI, providedEntityURI))
                    addedEntityProxies.add(otherProxy)
                    added = True
        if not entityProxy in addedEntityProxies:
            g.add((entityProxy, entityProxyForPropertyURI, providedEntityURI))
            addedEntityProxies.add(entityProxy)
            added = True

        if added:
            providedEntityCount = providedEntityCount + 1

    logger.info('Number of provided entities created: ' +
                str(providedEntityCount))

    # g.serialize(destination="providedEntities.ttl")

    logger.info('Entity provided updated')
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
    logger.info(len(data))
    return True

@task(log_stdout=True)
def get_start_time():
    schedule_time = prefect.context.get("scheduled_start_time")
    if schedule_time:
        return schedule_time
    else:
        datetime.now()

def create_source_entities(): 
    entities = []
    flow_run_id = prefect.context.flow_run_id
    params = prefect.context.parameters
    g = Graph()
    for index, source_graph in enumerate(params['entity_source_uris']):

        source_entity = URIRef(IDM_PROV[flow_run_id + '/source/' + str(index)])
        
        g.add( (source_entity, RDF.type, PROV.Entity) )
        g.add( (source_entity, IDM_PROV.source_graph, URIRef(source_graph)))
        entities.append(source_entity)

    # entity_enriched_uris
    id_source = URIRef(IDM_PROV[flow_run_id + '/source/id_source'])
    g.add( (id_source, RDF.type, PROV.Entity) )
    g.add( (id_source, IDM_PROV.source_graph, URIRef(params['entity_enriched_uris'])))
    entities.append(source_entity)

    return entities, g

def create_target_entities():
    entities = []
    flow_run_id = prefect.context.flow_run_id
    params = prefect.context.parameters
    g = Graph()

    target_graph = URIRef(IDM_PROV[flow_run_id + '/target'])
    g.add( (target_graph, RDF.type, PROV.Entity) )
    g.add( (target_graph, IDM_PROV.target_graph, URIRef(params['target_graph'])))
    entities.append(target_graph)

    return entities, g   

@task()
def add_provenance(_, start_time, create_source_entities, create_target_entities, endpoint):
    logger = prefect.context.get('logger')
    
    flow_name = prefect.context.flow_name
    flow_id = prefect.context.flow_id
    flow_run_id = prefect.context.flow_run_id    
    flow_run_version = prefect.context.get('flow_run_version', 'not-available')
    end_time = datetime.datetime.now()
    

    g = Graph()

    
    activityURI = URIRef(IDM_PROV['activity/' + flow_run_id])
    g.add((activityURI, RDF.type, PROV.Activity))
    g.add((activityURI, IDM_PREFECT.flow_name, Literal(flow_name)))
    g.add((activityURI, IDM_PREFECT.flow_id, Literal(flow_id)))
    g.add((activityURI, IDM_PREFECT.flow_run_version, Literal(flow_run_version)))
    g.add((activityURI, PROV.startedAtTime, Literal(start_time.isoformat(), datatype=XSD.dateTime)))
    g.add((activityURI, PROV.endedAtTime, Literal(end_time.isoformat(), datatype=XSD.dateTime)))
    # used 
    source_entities, source_entity_graph = create_source_entities()
    g = g + source_entity_graph
    for source_entity in source_entities:
        g.add( (activityURI, PROV.used, source_entity))


    # generated 
    target_entities, target_entity_graph = create_target_entities()
    g = g + target_entity_graph
    for target_entity in target_entities:
        g.add( (activityURI, PROV.generated, target_entity))

    
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    post_url =  endpoint + '?context-uri=' + PROV_TARGET_GRAPH + ''
    res2 = requests.post(post_url, headers={'Content-type': 'text/turtle'}, data=g.serialize(), auth=auth)


 
with Flow("Generate provided entity graph") as flow:
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql") # string
    entity_source_uris = Parameter('entity_source_uris', default=['https://apis.acdh.oeaw.ac.at/data', 'http://ldf.fi/nbf/data', 'http://data.biographynet.nl', 'http://data.biographynet.nl/places2wikidata/', 'http://www.intavia.eu/sbi', 'http://www.intavia.org/graphs/place-id-enrichment'])  # list
    entity_source_type = Parameter(
        "entity_source_type", default="http://www.cidoc-crm.org/cidoc-crm/E21_Person") # string
    entity_source_proxy_type = Parameter(
        "entity_source_proxy_type", default="http://www.intavia.eu/idm-core/Person_Proxy") # string
    entity_enriched_uris = Parameter(
        "entity_enriched_uris", default="http://www.intavia.org/graphs/person-id-enrichment") # string
    provided_entity_ns = Parameter(
        "provided_entity_ns", default="http://www.intavia.eu/provided_person/") # string
    provided_entity_type = Parameter(
        "provided_entity_type", default="http://www.intavia.eu/idm-core/Provided_Person") # string
    entity_proxy_for_property = Parameter(
        "entity_proxy_for_property", default="http://www.intavia.eu/idm-core/person_proxy_for") # string
    target_graph = Parameter(
        'target_graph', default='http://www.intavia.org/graphs/provided_persons') # string

    start_time = get_start_time()
    sparql = setup_sparql_connection(endpoint)
    id_graph = get_sameas_statements(sparql, entity_source_uris, entity_source_type, entity_source_proxy_type)
    provided_entities_graph = create_provided_entities_graph(
        sparql, id_graph, entity_enriched_uris, entity_source_type, entity_source_proxy_type, provided_entity_ns, provided_entity_type, entity_proxy_for_property)
    res = update_target_graph(endpoint, target_graph, provided_entities_graph)
    add_provenance(res, start_time, create_source_entities, create_target_entities, endpoint)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"},
                                job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows",
                      path="update_provided_entities.py")

# Persons
#flow.run(
#    endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#    #endpoint='https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql',
#    entity_source_uris=['https://apis.acdh.oeaw.ac.at/data', 'http://ldf.fi/nbf/data', 'http://data.biographynet.nl', 'http://data.biographynet.nl/places2wikidata/', 'http://www.intavia.eu/sbi', 'http://www.intavia.org/graphs/person-id-enrichment'],
#    entity_source_type="http://www.cidoc-crm.org/cidoc-crm/E21_Person",
#    entity_source_proxy_type="http://www.intavia.eu/idm-core/Person_Proxy",
#    entity_enriched_uris="http://www.intavia.org/graphs/person-id-enrichment",
#    provided_entity_ns="http://www.intavia.eu/provided_person/",
#    provided_entity_type="http://www.intavia.eu/idm-core/Provided_Person",
#    entity_proxy_for_property="http://www.intavia.eu/idm-core/person_proxy_for",
#    target_graph='http://www.intavia.eu/graphs/provided_persons'
#)

# Places
#flow.run(
#    endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#    #endpoint='https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql',
#    entity_source_uris=['https://apis.acdh.oeaw.ac.at/data', 'http://ldf.fi/nbf/data', 'http://data.biographynet.nl', 'http://data.biographynet.nl/places2wikidata/', 'http://www.intavia.eu/sbi', 'http://www.intavia.org/graphs/place-id-enrichment'],
#    entity_source_type="http://www.cidoc-crm.org/cidoc-crm/E53_Place",
#    entity_source_proxy_type="http://www.intavia.eu/idm-core/Place_Proxy",
#    entity_enriched_uris="http://www.intavia.org/graphs/place-id-enrichment",
#    provided_entity_ns="http://www.intavia.eu/provided_place/",
#    provided_entity_type="http://www.intavia.eu/idm-core/Provided_Place",
#    entity_proxy_for_property="http://www.intavia.eu/idm-core/place_proxy_for",
#    target_graph='http://www.intavia.eu/graphs/provided_places'
#)
