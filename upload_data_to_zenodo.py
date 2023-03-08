import os
import prefect
from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
import requests
from prefect.engine.signals import FAIL
from prefect.executors import LocalExecutor


TEMP_FOLDER = '/tmp/'


@task
def create_deposition(deposition_id):
    logger = prefect.context.get("logger")
    params = {'access_token': os.environ.get("ZENODO_API_TOKEN")}
    if deposition_id is None:
        logger.info("Creating new deposition")
        r = requests.post(
            'https://zenodo.org/api/deposit/depositions', params=params, json={})
        if r.status_code != 201:
            raise FAIL(f"Error creating deposition: {r.text}")
        return r.json()
    else:
        logger.info(
            f"Updating deposition {deposition_id}, creating new version")
        r = requests.post(
            f'https://zenodo.org/api/deposit/depositions/{deposition_id}/actions/newversion', params=params)
        if r.status_code != 200:
            raise FAIL("Error getting deposition: {}".format(r.text))
        return r.json()


@task
def update_deposition(metadata, deposition):
    logger = prefect.context.get("logger")
    if metadata is None:
        logger.info("No metadata provided, skipping update")
        return deposition
    params = {'access_token': os.environ.get("ZENODO_API_TOKEN")}
    logger.info(f"Updating deposition {deposition['id']}")
    r = requests.put(
        f'https://zenodo.org/api/deposit/depositions/{deposition["id"]}', params=params, json=metadata)
    if r.status_code != 200:
        raise FAIL("Error updating deposition: {}".format(r.text))
    return r.json()


@task
def upload_files(path, deposition):
    logger = prefect.context.get("logger")
    params = {'access_token': os.environ.get("ZENODO_API_TOKEN")}
    logger.info("Uploading files to deposition {}".format(deposition['id']))
    for filename in os.listdir(path):
        logger.info("Uploading file {}".format(filename))
        with open(os.path.join(path, filename), 'rb') as fp:
            r = requests.put(
                f"{deposition['links']['bucket']}/{filename}", params=params, data=fp)
            if r.status_code != 200:
                raise FAIL("Error uploading file: {}".format(r.text))
    return deposition


with Flow("Upload RDF files to Zenodo") as flow:
    deposition_id = Parameter("Deposition ID", default=None)
    path = Parameter("path to RDF file", )
    metadata = Parameter("metadata", default=None)

    deposition = create_deposition(deposition_id)
    deposition = update_deposition(metadata, deposition)
    upload_files(path, deposition)

# state = flow.run(executor=LocalExecutor(), parameters={
#                 "path to RDF file": "/workspaces/prefect-flows/testdata", "Deposition ID": "7708728"})

flow.run_config = KubernetesRun(
    env={"EXTRA_PIP_PACKAGES": "requests"}, job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows",
                      path="upload_data_to_zenodo.py")
