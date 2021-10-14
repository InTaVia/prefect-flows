from prefect import Flow, Parameter, task
from prefect.storage import GitHub
import os

@task(log_stdout=True)
def print_envs(a='test', b='test3'):
    for item, value in os.environ.items():
        print('{}: {}'.format(item, value))
        print(a)
        print(b)
    


with Flow("Print envs") as flow:
    print_envs()

flow.Storage = GitHub(repo="InTaVia/prefect-flows", path="printenv_flow.py")