from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from requests.auth import HTTPBasicAuth
import os

from prov_common import get_named_graphs_for_graph_ids


@task
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql

@task
def query_named_graphs(endpoint, timestamp):
    graphDict = get_named_graphs_for_graph_ids(endpoint, timestamp, None)
    results = {}
    if len(graphDict) > 0:
        # always just get the first key for now
        set_name = str(next(iter(graphDict)))
        versioned_graphs = list(graphDict[set_name].values())
        results[set_name] = versioned_graphs
    return results

def write_graph_to_file(endpoint, output_path, graph_uri):
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    retrieve_url =  endpoint + '?GETSTMTS&c=<' + graph_uri + '>'    
    res = requests.get(retrieve_url, auth=auth, headers={'Accept': 'text/plain'})
    with open(output_path, 'wb') as f:
        f.write(res.content)

@task
def do_a_dump(endpoint, data_dir, named_graphs, set_uri_prefix, version_uri_prefix, overwrite_if_exists):
    overwrite_if_exists = overwrite_if_exists == 'true'

    for set_name in named_graphs:
        filtered_set_name = set_name[len(set_uri_prefix)-1:]
        # check if the dir exists - if not -> process it
        export_dir = data_dir + filtered_set_name
        print(export_dir)
        if not os.path.exists(export_dir) or overwrite_if_exists:
            if not os.path.exists(export_dir):
                os.mkdir(export_dir)

            for named_graph in named_graphs[set_name]:
                graph_output_file = export_dir + '/' + named_graph[len(version_uri_prefix)+1:] + '.nq'
                write_graph_to_file(endpoint, graph_output_file, named_graph)            
        

with Flow("Data dump workflow") as flow:
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    timestamp = Parameter("timestamp") 
    data_dir = Parameter("data_dir", required=True)
    set_uri_prefix = Parameter('set_uri_prefix')
    version_uri_prefix = Parameter('version_uri_prefix')
    overwrite_if_exists = Parameter('overwrite_if_exists')
    sparql_wrapper = setup_sparql_connection(endpoint)
    named_graphs = query_named_graphs(endpoint, timestamp)
    do_a_dump(endpoint, data_dir, named_graphs, set_uri_prefix, version_uri_prefix, overwrite_if_exists)

flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "requests rdflib SPARQLWrapper"})
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="datadump_workflow.py")

# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     timestamp=None,
#     data_dir='/Users/kesanij1/Documents/temp/dump/',
#     set_uri_prefix='http://www.intavia.eu/sets/',
#     version_uri_prefix='http://intavia.eu/graphs/test_dataset/version/',
#     overwrite_if_exists='true'
# )