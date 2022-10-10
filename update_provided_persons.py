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

IDM_PROVIDED_PERSON = 'http://www.intavia.eu/provided_person/'
IDMCORE = Namespace("http://www.intavia.eu/idm-core/")
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
def load_id_graph(sparql, id_source_graph):
    logger = prefect.context.get('logger')
    loadQuery = """  
  construct {
    ?s ?p ?o
  }
  FROM <""" + id_source_graph + """>
  where {
    ?s ?p ?o
  }
  """

    sparql.setQuery(loadQuery)
    results = sparql.queryAndConvert()

    with open('/tmp/lg_data.ttl', 'w') as file:
        file.write(results.serialize())

    g = Graph()
    g.parse('/tmp/lg_data.ttl')

    logger.info('Linking data loaded')

    return g


@task
def create_provided_persons_graph(sparql, id_graph, person_source_uris):
    logger = prefect.context.get('logger')

    from_part = '\n'.join(
        list(map(lambda x: 'from <' + x + '>', person_source_uris)))
    personQuery = """
  select ?person
  """ + from_part + """
  where 
  {
    ?person a <http://www.cidoc-crm.org/cidoc-crm/E21_Person>
  } 
  """

    g = Graph()

    providedPersonType = IDMCORE.Provided_Person
    addedPersonProxies = set()

    providedPersonCount = 1
    sparql.setQuery(personQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    for result in results["results"]["bindings"]:
        personProxyURI = result['person']['value']
        check = False
        personProxy = URIRef(personProxyURI)
        providedPersonURI = URIRef(
            IDM_PROVIDED_PERSON + str(providedPersonCount))
        g.add((providedPersonURI, RDF.type, providedPersonType))
        # check for owl:sameAs links
        added = False
        for extID in id_graph.objects(personProxy, OWL.sameAs):
            for otherProxy in id_graph.subjects(OWL.sameAs, extID):
                if not otherProxy in addedPersonProxies:
                    g.add((otherProxy, IDMCORE.person_proxy_for, providedPersonURI))
                    addedPersonProxies.add(otherProxy)
                    added = True
        if not personProxy in addedPersonProxies:
            g.add((personProxy, IDMCORE.person_proxy_for, providedPersonURI))
            addedPersonProxies.add(personProxy)
            added = True

        if added:
            providedPersonCount = providedPersonCount + 1

    logger.info('Number of provided persons created: ' +
                str(providedPersonCount))

    # g.serialize(destination="providedPersons.ttl")

    logger.info('Person provided updated')
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
    for index, source_graph in enumerate(params['person_source_uris']):

        source_entity = URIRef(IDM_PROV[flow_run_id + '/source/' + str(index)])
        
        g.add( (source_entity, RDF.type, PROV.Entity) )
        g.add( (source_entity, IDM_PROV.source_graph, URIRef(source_graph)))
        entities.append(source_entity)

    # id_source_uri
    id_source = URIRef(IDM_PROV[flow_run_id + '/source/id_source'])
    g.add( (id_source, RDF.type, PROV.Entity) )
    g.add( (id_source, IDM_PROV.source_graph, URIRef(params['id_source_uri'])))
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


 
with Flow("Generate provided person graph") as flow:
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    id_source_uri = Parameter(
        'id_source_uri', default='http://www.intavia.org/graphs/person-id-enrichment')  # string
    person_source_uris = Parameter('person_source_uris', default=[
                                   'http://www.intavia.org/graphs/apis', 'http://www.intavia.org/graphs/bs'])  # list
    target_graph = Parameter(
        'target_graph', default='http://www.intavia.org/graphs/provided_persons')  # string

    start_time = get_start_time()
    sparql = setup_sparql_connection(endpoint)
    id_graph = load_id_graph(sparql, id_source_uri)
    provided_persons_graph = create_provided_persons_graph(
        sparql, id_graph, person_source_uris)
    res = update_target_graph(endpoint, target_graph, provided_persons_graph)
    add_provenance(res, start_time, create_source_entities, create_target_entities, endpoint)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"},
                                job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows",
                      path="update_provided_persons.py")

# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     person_source_uris=['http://www.intavia.org/graphs/apis', 'http://www.intavia.org/graphs/bs'],
#     target_graph='http://www.intavia.org/graphs/provided_persons',
#     id_source_uri='http://www.intavia.org/graphs/person-id-enrichment'
# )