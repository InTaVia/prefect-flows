from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
import prefect
import requests
import rdflib
from rdflib.namespace import OWL, RDF, RDFS

import owlrl
from requests.auth import HTTPBasicAuth
import os


TEMP_FOLDER='/tmp/'

def retrive_named_graph(endpoint, graph_uri):
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    retrieve_url =  endpoint + '?GETSTMTS&c=<' + graph_uri + '>'    
    res = requests.get(retrieve_url, auth=auth, headers={'Accept': 'text/plain'})
    g = rdflib.ConjunctiveGraph()
    
    g.parse( data=res.text, format='nquads' )
    return g

@task
def read_source_data(endpoint, source_graph_uris):
    g = rdflib.ConjunctiveGraph()
    for graph_uri in source_graph_uris:
        subgraph = retrive_named_graph(endpoint, graph_uri)
        g =g + subgraph
    print(len(g))
    return g

@task
def owl_inferencing(source_graph, inference_type, ontology_source_type, ontology_source_value):

    if inference_type != 'rdfs':
        raise Exception('Only RDF inference type is supported for now!')
    if ontology_source_type != 'url':
        raise Exception('Only url ontology source type is supported for now!')
    ontology_graph = rdflib.Graph()
    ontology_graph.parse(ontology_source_value, format='turtle')
    new_graph = source_graph + ontology_graph
    print(len(new_graph))
    owl = owlrl.DeductiveClosure(owlrl.RDFSClosure.RDFS_Semantics, False, False, True)
    owl.expand(new_graph)
    print(len(new_graph))
    result_graph = new_graph - source_graph
    result_graph = result_graph - ontology_graph
    print(len(result_graph))
    return result_graph

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


with Flow("Inference workflow") as flow:
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")

    source_graph_uris = Parameter("source_graph_uris") # list
    target_graph_uri = Parameter('target_graph_uri')  # string

    inference_type = Parameter('inference_type', default='rdfs')
    ontology_source_type = Parameter('ontology_source_type', default="url")
    ontology_source_value = Parameter('ontology_source_value')

    source_graph = read_source_data(endpoint, source_graph_uris)
    inferred_triples = owl_inferencing(source_graph, inference_type, ontology_source_type, ontology_source_value)
    update_target_graph(endpoint, target_graph_uri, inferred_triples)



flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "requests rdflib owlrl"})
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="inference_workflow.py")

# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     source_graph_uris=['http://www.intavia.org/graphs/provided_persons'],
#     target_graph_uri = 'http://www.intavia.org/graphs/inference_test',
#     ontology_source_value= 'https://raw.githubusercontent.com/InTaVia/prefect-flows/master/testdata/test_rdfs_ontology1.ttl'
# )