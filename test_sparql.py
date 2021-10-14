from prefect import Flow, Parameter, task
from prefect.storage import GitHub


@task(log_stdout=True)
def print_total(x, y, total):
    print(f"{x} + {y} = {total}")


with Flow("Example: Parameters") as flow:
    x = Parameter("x", default=1)
    y = Parameter("y", default=2)

    print_total(x, y, x + y)

flow.Storage = GitHub(repo="InTaVia/prefect-flows", path="test.py")