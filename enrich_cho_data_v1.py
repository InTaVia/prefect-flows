
from datetime import timedelta
from string import Template
from prefect import task, Flow, Parameter, context
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import requests


TEMP_FOLDER = '/tmp/'

@task(log_stdout=True)
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


@task(log_stdout=True)
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql

@task(log_stdout=True)
def retrieve_counts(sparql):
    query = """
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT (COUNT(?person) AS ?count)
    
    WHERE {
        ?person a crm:E21_Person .
        ?person owl:sameAs ?personUri .
  FILTER(contains(str(?personUri), "wikidata.org"))
    }       
        """ 
    logger = context.get("logger")
    logger.info(f"Getting absolut counts of person entities with wiki data links")
    sparql.setQuery(query)
    try:
        results = sparql.query().convert()
    except Exception as e:
        logger.error(f"Error while retrieving counts: {e}")
        raise e
    return int(results["results"]["bindings"][0]["count"]["value"])

@task(log_stdout=True, max_retries=3, retry_delay=timedelta(seconds=360))
def retrieve_cho_data(sparql, offset, limit, template, named_graph):
    logger = context.get("logger")
    logger.info(f"Retrieving data from {offset} to {offset + limit}")
    with open(template, "r+") as query:
        st1 = Template(query.read()).substitute(namedGraph=named_graph, offset=offset, limit=limit)
    sparql.setQuery(st1)
    try:
        results = sparql.queryAndConvert()
    except Exception as e:
        logger.error(f"Error while retrieving counts: {e}")
        raise e
    return results

@task(log_stdout=True)
def retrieve_cho_data_master(sparql, limit, template, named_graph, max_entities):
    if max_entities is None:
        max_entities = retrieve_counts.run(sparql)
    offset = 0
    while offset < max_entities:
        results = retrieve_cho_data.run(sparql, offset, limit, template["convert_cho_wikidata_v1.sparql"], named_graph)
        offset += limit
    return results


with Flow("InTaVia CHO Wikidata") as flow:
    endpoint = Parameter("SPARQL Endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    limit = Parameter("Limit", default=100)
    max_entities = Parameter("Max Entities", default=None)
    named_graph = Parameter("Named Graph", default="http://data.acdh.oeaw.ac.at/intavia/cho")
    sparql = setup_sparql_connection(endpoint)
    temp_files = download_source_data(["https://raw.githubusercontent.com/InTaVia/prefect-flows/master/sparql/convert_cho_wikidata_v2.sparql"])
    res = retrieve_cho_data_master(sparql, limit, temp_files, named_graph, max_entities)


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="enrich_cho_data_v1.py")