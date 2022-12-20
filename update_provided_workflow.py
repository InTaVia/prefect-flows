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
def create_provided_graph(sparql, id_graph, source_uris, source_type, target_type, target_uri_prefix, proxy_for_prop_uri):
    logger = prefect.context.get('logger')

    from_part = '\n'.join(
        list(map(lambda x: 'from <' + x + '>', source_uris)))
    query = """
  select ?resource
  """ + from_part + """
  where 
  {
    ?resource a <""" + source_type + """>
  } 
  """

    g = Graph()

    providedType = URIRef(target_type)
    addedProxies = set()

    providedCount = 1
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    for result in results["results"]["bindings"]:
        proxyURI = result['resource']['value']
        proxy = URIRef(proxyURI)
        providedURI = URIRef(
            target_uri_prefix + str(providedCount))
        g.add((providedURI, RDF.type, providedType))
        # check for owl:sameAs links
        added = False
        for extID in id_graph.objects(proxy, OWL.sameAs):
            for otherProxy in id_graph.subjects(OWL.sameAs, extID):
                if not otherProxy in addedProxies:
                    g.add((otherProxy, proxy_for_prop_uri, providedURI))
                    addedProxies.add(otherProxy)
                    added = True
        if not proxy in addedProxies:
            g.add((proxy, proxy_for_prop_uri, providedURI))
            addedProxies.add(proxy)
            added = True

        if added:
            providedCount = providedCount + 1

    logger.info('Number of provided resources created: ' +
                str(providedCount))

    # g.serialize(destination="providedResources.ttl")

    logger.info('Provided resources updated')
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

 
with Flow("Generate provided resource graph") as flow:
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    id_source_uri = Parameter(
        'id_source_uri')  # string
    source_named_graphs = Parameter('source_named_graphs', default=[
                                   'http://www.intavia.org/graphs/apis', 'http://www.intavia.org/graphs/bs'])  # list
    source_type_uri = Parameter('source_type_uri') # string    
    target_graph = Parameter(
        'target_graph')  # string

    target_type_uri = Parameter('target_type_uri')
    target_uri_prefix = Parameter('target_uri_prefix')
    proxy_for_prop_uri = Parameter('proxy_for_prop_uri')
    sparql = setup_sparql_connection(endpoint)
    id_graph = load_id_graph(sparql, id_source_uri)    
    provided_graph = create_provided_graph(
        sparql, id_graph, source_named_graphs, source_type_uri, target_type_uri, target_type_uri, proxy_for_prop_uri)
    res = update_target_graph(endpoint, target_graph, provided_graph)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"},
                               job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows",
                     path="update_provided.py")

# # Provided persons
# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     source_named_graphs=['http://www.intavia.org/graphs/apis', 'http://www.intavia.org/graphs/bs'],
#     source_type_uri = 'http://www.cidoc-crm.org/cidoc-crm/E21_Person',
#     target_graph='http://www.intavia.org/graphs/provided_persons',
#     target_type_uri='http://www.intavia.eu/idm-core/Provided_Person',
#     target_uri_prefix='http://www.intavia.eu/provided_person/',
#     proxy_for_prop_uri='http://www.intavia.eu/idm-core/person_proxy_for',
#     id_source_uri='http://www.intavia.org/graphs/person-id-enrichment'
# )

# # Provided places
# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     source_named_graphs=['http://www.intavia.org/graphs/apis', 'http://www.intavia.org/graphs/bs'],
#     source_type_uri = 'http://www.cidoc-crm.org/cidoc-crm/E53_Place',
#     target_graph='http://www.intavia.org/graphs/provided_places',
#     target_type_uri='http://www.intavia.eu/idm-core/Provided_Place',
#     target_uri_prefix='http://www.intavia.eu/provided_place/',
#     proxy_for_prop_uri='http://www.intavia.eu/idm-core/place_proxy_for',
#     id_source_uri='http://www.intavia.org/graphs/place-id-enrichment'
# )