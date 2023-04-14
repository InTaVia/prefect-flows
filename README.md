# What is this repository?
The [InTaVia](https://intavia.eu) Knowledge Graph (KB) is combining data from 4 national biographies (Austria, Finland, The Netherlands and Slovenia) and references resources such as [wikidata](https://wikidata.org) and [Europeana](https://euopeana.eu). To make the data reproducible  - in the sense of "check out the commit from YYYY-MM-DD and run script A to reproduce the state of the KB valid at this day" - and easier to handle - there is simply to much data to do any manual curation and/or validity check - we came up with a plugin system.
This system is built around [Prefect.io](https://prefect.io). [Prefect.io](https://prefect.io) is a workflow orchestration system that allows to build complex data processing pipelines and execute them on various events.
While [Prefect.io](https://prefect.io) itself allows for various setups, our plugin system is deployed in the [ACDH-CH](https://acdh.oeaw.ac.at) Kubernetes Cluster. Every job that gets submitted to the pipeline fires up a new container which is teared down as soon as the job has finished. The workflows are stored in this repo and the up-to-date code is fetched on every run. This secures the simple interaction of the InTaVia development team with the plugin system (every developer with access to the repo can update the plugins).
This repo contains the various flows that have been developed so far, as well as a job-template that allows us to pass kubernetes-secrets to the job itself.

# Overview of the flows
In this section we briefly describe the purpose and structure of the flows.

## Validate dataset (not available yet)
We are currently working on a [ShEx](https://shex.io) schema for validating the datasets before ingesting them. As soon as this is ready we will add a plugin that validates new (versions of) datasets against this schema and stops the ingestion process in case the file(s) do not validate.

## Ingest (mock data) workflow(partly available)
The ingest workflows (only the mock data one is currently published) downloads data from a given location (currently a GitHub repository) and uploads it to a configurable triplestore in a configurable named graph.

## Inference workflow(available in a first version)
In the current setup the InTaVia Knowledge Graph uses a blazegraph triplestore in quad mode (to allow for named graphs). Blazegraph does not allow for inference in quad mode, we therefore generate inference triples with this plugin and push them to the triplestore.

## Person id linker(available in a first version)
This plugin uses reference resources URIs (such as GND and wikidata) to find the corresponding person in wikidata.org. In a second step it uses the wikidata object to retrieve missing reference resource URIs and adds them to the KB. This is an important step as datasets very often use different reference resources to identify entities in there datasets. E.g. the Austrian data (ÖBL) uses GND identifiers, while BiographySampo uses wikidata.

## Enrich cho data (available in a first version)
This plugin uses the wikidata identifier added by the "Person id linker" plugin to search wikidata for persons and then downloads cultural heritage objects linked to these persons from wikidata. Before ingesting it into the InTaVia KB it converts the data to the IDM-RDF datamodel. To avoid timeouts it has a configurable number of persons it works on in parallel and it also allows to set the target named graph.

## Enrich wikidata interperson relations (available in a first version)
This plugin uses the wikidata identifier added by the "Person id linker" plugin to search wikidata for interperson relations connected the people in InTaVia dataset. The relations can be genealogical (parent, child, spouse, ...), educations (teacher, student, supervisor, ..), or related to career (co-worker, influencer, ...). Current implementations first queries the Wikidata identifiers from the InTaVia triplestore, and uses the found links to extract the results from Wikidata triplestore.

# Upcoming improvements
During work on the flows we came across several shortcomings of the structure we had in mind when designing the plugin system.

## Reusable tasks
Every plugin (flow in the sense of prefect) consists of several tasks that are triggered in a certain sequence and/or by certain events (such as `result of task A = B`). However, a lot of these tasks are rather simple and generic: e.g. fetch a SPARQL query from location A and return it. Currently we copy those tasks between the flows (as a simple import is due to our setup not possible), but plan on packing those generic tasks into a module which gets installed in every plugin.

## flows of flows
Currently every plugin (flow) gets triggered and executed on its own. However, we are working on a flow that controls all the other flows depending on the state of the Knowledge Graph and certain events. E.g.: if there is a new version of a dataset available it will start the ingestion plugin, after that the inference plugin, then the enrichment plugin etc. By implementing that we will secure a better separation of concerns: the plugins themselves need to care about changing the KB only and not about when to run etc. The "orchestration flow" on the other hand will only listen on events and trigger the plugins accordingly. This secures also that dependencies between events need to be dealt with only in the orchestration flow.


# Usage
## Using custom job template

[Docs](https://docs.prefect.io/orchestration/agents/kubernetes.html#custom-job-template)

You can add custom job template either on the agent or job level. Agent level template can be added to the agent start command.

```
prefect agent kubernetes start --job-template /intavia-job-template.yaml
```

Or you can add to the run_config of a single job. 

```
flow.run_config = KubernetesRun(job_template_path="/intavia-job-template.yaml")
```

### Local dev

`poetry install`

`poetry run SCRIPT.PY`

#### person_id_linker

* Comment out the lines for configuring the flow on the Kubernetes cluster: flow.run_config & flow.storage
* Uncomment the line for running the flow locally: flow.run()

If you wish to serialize the data in file instead of storing the data in a named graph on SPARQL server:
* Comment out the line for updating target graph.
* Uncomment the line for serializing graph into file.

`RDFDB_USER=... RDFDB_PASSWORD=... poetry run python person_id_linker.py`
