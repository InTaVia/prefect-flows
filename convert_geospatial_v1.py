from datetime import timedelta
from .enrich_cho_data_v1 import download_source_data, setup_sparql_connection
from SPARQLWrapper import SPARQLWrapper, JSON
from prefect import task, Flow, Parameter, context
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun

TEMP_FOLDER = '/tmp/'

@task(log_stdout=True, max_retries=3, retry_delay=timedelta(seconds=360))
def run_sparql_insert(sparql, query):
    logger = context.get("logger")
    logger.info(f"Executing query")
    sparql.setQuery(query)
    try:
        sparql.query()
    except Exception as e:
        logger.error(f"Error while executing query: {e}")
        raise e

with Flow("InTaVia convert spatial coordinates to blazegraph"):
    named_graph = Parameter("Named graph", default="http://data.acdh.oeaw.ac.at/intavia/spatial")
    endpoint = Parameter("SPARQL Endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    sparql = setup_sparql_connection(endpoint)
    temp_files = download_source_data(["https://raw.githubusercontent.com/InTaVia/prefect-flows/master/sparql/add_geoliterals_blazegraph_v1.sparql"])