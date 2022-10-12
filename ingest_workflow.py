from prefect import Flow
from common import get_start_time_task



with Flow("Ingest workflow") as flow:
    start_time = get_start_time_task()


    



flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "requests"})
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="ingest_workflow.py")

#flow.run(
    #endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
    #source_graph_uris=['http://www.intavia.org/graphs/provided_persons'],
    #target_graph_id='http://intavia.eu/graphs/test',
#)