# What is this repository?


# Using custom job template

[Docs](https://docs.prefect.io/orchestration/agents/kubernetes.html#custom-job-template)

You can add custom job template either on the agent or job level. Agent level template can be adde to the agent start command.

```
prefect agent kubernetes start --job-template /intavia-job-template.yaml
```

Or you can add to the run_config of a single job. 

```
flow.run_config = KubernetesRun(job_template_path="/intavia-job-template.yaml")
```

## Local dev

`poetry install`

`poetry run SCRIPT.PY`

### person_id_linker

* Comment out the lines for configuring the flow on the Kubernetes cluster: flow.run_config & flow.storage
* Uncomment the line for running the flow locally: flow.run()

If you wish to serialize the data in file instead of storing the data in a named graph on SPARQL server:
* Comment out the line for updating target graph.
* Uncomment the line for serializing graph into file.

`RDFDB_USER=... RDFDB_PASSWORD=... poetry run python person_id_linker.py`