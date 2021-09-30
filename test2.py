# flows/my_flow.py

from prefect import task, Flow
from prefect.storage import GitHub

@task
def get_data():
    return [1, 2, 3, 4, 5]

@task
def print_data(data):
    print(data)

with Flow("example") as flow:
    data = get_data()
    print_data(data)

flow.storage = GitHub(
    repo="InTaVia/prefect-flows", path="test.py",                 # location of flow file in repo
    access_token_secret="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"   # name of personal access token secret
)
