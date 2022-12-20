from prefect import task, Flow, Parameter
import prefect
from pyshex.shex_evaluator import ShExEvaluator
from pyshex.user_agent import SlurpyGraphWithAgent, SPARQLWrapperWithAgent
from pyshex.utils.sparql_query import SPARQLQuery
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import jsonasobj
from rdflib import URIRef

def query_focus_nodes(sparql, target_named_graph, validated_resource_type):
    query = """
SELECT DISTINCT ?item FROM <""" + target_named_graph + """> WHERE {
  ?item a <""" + validated_resource_type + """>
}
"""   
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    result = sparql.query()
    processed_results = jsonasobj.load(result.response)
    return [URIRef(row.item.value) for row in processed_results.results.bindings]


@task
def validate_dataset(sparql, target_named_graph, validated_resource_type, shex):
    logger = prefect.context.get('logger')
    logger.info(endpoint)
    focus_nodes = query_focus_nodes(sparql, target_named_graph, validated_resource_type)
    result = ShExEvaluator(sparql,
                       shex,
                       focus_nodes).evaluate()
    for r in result:
        logger.info(f"{r.focus}: ")
        if not r.result:
            logger.error(f"FAIL: {r.reason}")
            raise Exception(f"Validation error. Resource: {r.focus}")
        else:
            logger.info("PASS")        
@task
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(user=os.environ.get(
        "RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD"))
    return sparql

with Flow("ShEx validation workflow") as flow:
    endpoint = Parameter(
        "endpoint", default="https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql")

    target_graph = Parameter("target_graph") 
    target_type = Parameter("target_type") 
    shex = Parameter('shex')
    
    sparql_wrapper = setup_sparql_connection(endpoint)
    validate_dataset(sparql_wrapper, target_graph, target_type, shex)

flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "requests rdflib pyshex jsonasobj"})
flow.storage = GitHub(repo="InTaVia/prefect-flows", path="shex_validation_workflow.py")


# flow.run(
#     endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#     target_graph = 'http://intavia.eu/graphs/test_dataset/version/8d8d240f-cfbb-417e-bcaa-0d5410adab20',
#     target_type = 'http://www.cidoc-crm.org/cidoc-crm/E36_Visual_Item',
#     shex = """
# START=@:testShape
# <testShape> {
#   <http://www.cidoc-crm.org/cidoc-crm/P138_represents> xsd:string ?
# }    
#     """
# )