from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import URIRef, Namespace, Graph
from rdflib.namespace import OWL, RDF
import prefect
from prefect import Flow, task, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
import requests
import os
IDM_PROVIDED_PERSON = 'http://www.intavia.eu/provided_person/'
IDMCORE = Namespace("http://www.intavia.eu/idm-core/")


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

  from_part = '\n'.join(list(map(lambda x : 'from <' + x +'>', person_source_uris) ))
  personQuery = """
  select ?person
  """ + from_part + """
  where 
  {
    ?person a <http://www.cidoc-crm.org/cidoc-crm/E21_Person>
  } 
  """

  logger.info(personQuery)

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
    providedPersonURI = URIRef(IDM_PROVIDED_PERSON + str(providedPersonCount))
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
      
  logger.info('Number of provided persons created: ' + str(providedPersonCount))

  #g.serialize(destination="providedPersons.ttl")  

  logger.info('Person provided updated')
  return g

@task()
def update_target_graph(endpoint, target_uri, data):
  logger = prefect.context.get('logger')
  logger.info(len(data))
  delete_url =  endpoint + '?c=<' + target_uri + '>'
  post_url =  endpoint + '?context-uri=' + target_uri + ''
  requests.delete(delete_url)
  requests.post(post_url, headers={'Content-type': 'text/turtle'}, data=data.serialize())

with Flow("Generate provided person graph") as flow:
  endpoint = Parameter("endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
  id_source_uri = Parameter('id_source_uri', default='http://www.intavia.org/graphs/person-id-enrichment') # string
  person_source_uris = Parameter('person_source_uris', default=['http://www.intavia.org/graphs/apis', 'http://www.intavia.org/graphs/bs']) # list
  target_graph = Parameter('target_graph', default='http://www.intavia.org/graphs/provided_persons') # string

  sparql = setup_sparql_connection(endpoint)
  id_graph = load_id_graph(sparql, id_source_uri)
  provided_persons_graph = create_provided_persons_graph(sparql, id_graph, person_source_uris)
  update_target_graph(endpoint, target_graph, provided_persons_graph)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="update_provided_persons.py")
