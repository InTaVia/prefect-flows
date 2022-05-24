
from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
import requests
import shutil
import testdata.functions as functions


TEMP_FOLDER = '/tmp/'

@task
def download_source_data(sources):
    local_files = {}
    for source in sources:
        local_filename = source.split('/')[-1]
        target_file = TEMP_FOLDER + local_filename
        r = requests.get(source, allow_redirects=True)
        with open(target_file, 'w') as f:
            f.write(r.text)
        local_files[local_filename] = target_file
    return local_files

@task(nout=2)
def read_data(local_files):
    return functions.read_data(local_files)


@task 
def create_graph(pldf, chodf):
    return functions.create_graph(pldf, chodf)

@task
def write_graph(g):
    return functions.wrte_graph(TEMP_FOLDER + 'graph.ttl', g)


with Flow("Mock data ingest flow") as flow:
    sources = Parameter("Sources")

    local_files = download_source_data(sources)
    pldf, chodf = read_data(local_files)
    g = create_graph(pldf, chodf)
    write_graph(g)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "pandas rdflib lxml SPARQLWrapper"}, labels=["intavia"])
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="ingest_mock_data_flow.py")