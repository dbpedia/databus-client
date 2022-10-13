#!/usr/bin/env python3
import typer
from typing import List
from databusclient import client

app = typer.Typer()


@app.command()
def deploy(
    version_id: str = typer.Option(
        ...,
        help="target databus version/dataset identifier of the form "
             "<https://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION>"),
    title: str = typer.Option(..., help="dataset title"),
    abstract: str = typer.Option(..., help="dataset abstract max 200 chars"),
    description: str = typer.Option(..., help="dataset description"),
    license_uri: str = typer.Option(..., help="license (see dalicc.net)"),
    apikey: str = typer.Option(..., help="apikey"),
    distributions: List[str] = typer.Argument(
        ...,
        help="distributions in the form of List[URL|CV|fileext|compression|sha256sum:contentlength] where URL is the "
             "download URL and CV the "
             "key=value pairs (_ separted) content variants of a distribution. filext and compression are optional "
             "and if left out inferred from the path. If the sha256sum:contentlength part is left out it will be "
             "calcuted by downloading the file.",
    ),
):
    typer.echo(version_id)
    dataid = client.createDataset(
        version_id, title, abstract, description, license_uri, distributions
    )
    client.deploy(dataid=dataid, api_key=apikey)


@app.command()
def download(collection: str):
    typer.echo(f"TODO")
