
from string import Template
from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from SPARQLWrapper import SPARQLWrapper, JSON
import os


@task
def setup_sparql_connection(endpoint):
    sparql_endpoint = os.environ.get("SPARQL_ENDPOINT")
    sparql = SPARQLWrapper(sparql_endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))

@task
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
    sparql.setQuery(query)
    results = sparql.query().convert()
    return int(results["results"]["bindings"][0]["count"]["value"])

@task
def retrieve_cho_data(sparql, offset, limit, template, named_graph):
    with open(template, "r+") as query:
        st1 = Template(query.read()).substitute(namedGraph=named_graph, offset=offset, limit=limit)
    sparql.setQuery(st1)
    results = sparql.queryAndConvert()
    return results


with Flow("Example flow path") as flow:
    endpoint = Parameter("SPARQL Endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")
    limit = Parameter("Limit", default=100)
    max_entities = Parameter("Max Entities", default=None)
    named_graph = Parameter("Named Graph", default="http://data.acdh.oeaw.ac.at/intavia/cho")
    sparql = setup_sparql_connection(endpoint)
    if max_entities is None:
        max_entities = retrieve_counts(sparql)
    offset = 0
    while offset < max_entities:
        results = retrieve_cho_data(sparql, offset, limit, "sparql/convert_cho_wikidata_v1.sparql", named_graph)
        offset += limit


flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="enrich_cho_data_v1.py")