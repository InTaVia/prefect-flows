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