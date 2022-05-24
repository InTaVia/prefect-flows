from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun


TEMP_FOLDER = '/tmp/'

@task
def download_source_data(sources):
    local_files = {}
    print("run executed")
    return local_files

@task(nout=2)
def read_data(local_files):
    return "test", "test2"


with Flow("Example flow path") as flow:
    sources = Parameter("Sources")

    local_files = download_source_data(sources)
    pldf, chodf = read_data(local_files)
    print(pldf, chodf)

flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "pandas rdflib lxml SPARQLWrapper"}, job_template_path="intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="example_job_template.py")