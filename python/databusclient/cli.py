#!/usr/bin/env python3
import typer
from typing import List
from databusclient import client

app = typer.Typer()


@app.command()
def deploy(
    versionId: str = typer.Option(..., help="target databus version/dataset identifier of the form <https://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION>"),
    title: str = typer.Option(..., help="dataset title"), 
    abstract: str = typer.Option(..., help="dataset abstract max 200 chars"), 
    description: str = typer.Option(..., help="dataset description"), 
    license: str = typer.Option(..., help="license (see dalicc.net)"),
    apikey: str = typer.Option(..., help="apikey"),
    distributions: List[str] = typer.Argument(..., help="distributions in the form of List[URL|CV|fileext|compression] where URL is the download URL and CV the key=value pairs (_ separted) content variants of a distribution. filext and compression are optional and if left out inferred from the path.")
):
    typer.echo(versionId)
    dataid = client.createDataset(versionId,title,abstract,description,license,distributions)
    client.deploy(dataid=dataid,api_key=apikey)


@app.command()
def download(collection: str):
    typer.echo(f"TODO")

app()
