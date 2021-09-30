"""
# Parameters

A `Parameter` is a special type of `Task` representing an input that can vary
per flow run. For example:

```python
x = Parameter("x", default=1)
```

Parameters have a name (`"x"` in this case), and may optionally include a
default value. Parameters lacking a default value require an explicit value be
configured for each flow run. Parameters with a default value may use the
default, or optionally provide a different value at runtime. Parameters can be
specified through the UI or CLI when running with Prefect Cloud/Server (see
[here](/orchestration/tutorial/parameters.md)) or through the `parameters`
kwarg when running locally with `flow.run`.

For more information, see the [Parameter docs](/core/concepts/parameters.md).
"""

from prefect import Flow, Parameter, task
from prefect.storage import GitHub


@task(log_stdout=True)
def print_total(x, y, total):
    print(f"{x} + {y} = {total}")


with Flow("Example: Parameters") as flow:
    x = Parameter("x", default=1)
    y = Parameter("y", default=2)

    print_total(x, y, x + y)

flow.Storage = GitHub(repo="InTaVia/prefect-flows", path="test.py", access_token_secret="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")
