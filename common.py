import datetime
import prefect
from prefect import task
import rdflib
import requests
from requests.auth import HTTPBasicAuth
import os

@task(log_stdout=True)
def get_start_time_task():
    schedule_time = prefect.context.get("scheduled_start_time")
    if schedule_time:
        return schedule_time
    else:
        datetime.datetime.now()

def retrive_named_graph(endpoint, graph_uri):
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    retrieve_url =  endpoint + '?GETSTMTS&c=<' + graph_uri + '>'    
    res = requests.get(retrieve_url, auth=auth, headers={'Accept': 'text/plain'})
    g = rdflib.ConjunctiveGraph()
    
    g.parse( data=res.text, format='nquads' )
    
    return g