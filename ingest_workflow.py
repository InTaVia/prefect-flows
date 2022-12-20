from prov_common import generate_versioned_named_graph, add_provenance_data
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect import Flow, task, Parameter
import prefect
import tempfile
import requests

import os

@task
def add_data_from_url(endpoint, source_url, source_type, target_graph_uri):
    logger = prefect.context.get('logger')
    res = requests.get(source_url)
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    post_url =  endpoint + '?context-uri=' + target_graph_uri + ''
    logger.info(post_url)
    res = requests.post(post_url, headers={'Content-type': source_type}, data=res.text.encode('utf-8'), auth=auth)
    logger.info(res)

    return True

with Flow("Ingest workflow") as flow:    
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    source_url = Parameter(
        "source_url")
    source_label = Parameter(
        "source_label")

    target_graph_id = Parameter(
        "target_graph_id")
    target_label = Parameter(
        "target_label")
    source_type = Parameter(
        "source_type")
    
    target_named_graph_uri = generate_versioned_named_graph(target_graph_id)
    res = add_data_from_url(endpoint, source_url, source_type, target_named_graph_uri)
    add_provenance_data(res, 
        endpoint,
        [
            {
                'label': source_label,
                'uri': source_url         
            }
        ],
        [
            {
                'label': target_label,
                'uri': target_named_graph_uri
            }
        ]           
    )



flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "requests"})
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="ingest_workflow.py")

# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     source_url='https://raw.githubusercontent.com/InTaVia/idm-rdf/main/test_dataset_designsprint/exdataset.ttl',
#     source_type='text/turtle',
#     source_label='Test dataset',
#     target_graph_id='http://intavia.eu/graphs/test_dataset',
#     target_label='Test data'
# )
