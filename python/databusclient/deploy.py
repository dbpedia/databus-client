from dataclasses import dataclass
from distutils import extension
from distutils.log import debug
from html.entities import name2codepoint
from tokenize import group
from typing import List
import requests
import hashlib
import json

# @dataclass
# class Dataset:
#     id: str
#     title: str
#     abstract: str
#     documentation: str
#     distributions: List[]

__debug = True

def __getCV(downloadUrl):
    lastSegment = str(downloadUrl).split("/")[-1]
    lastSegementSplits = lastSegment.split("|")
    
    cvDefinition = ""
    if len(lastSegementSplits) > 1:
        cvDefinition = "_"+lastSegementSplits[1].strip("_")

    return(cvDefinition)


def __getExtensions(downloadUrl):

    extensionPart = ""
    formatExtension = "file"
    compression = "none"

    # TODO redundant
    lastSegment = str(downloadUrl).split("|")[-1].split("/")[-1]
    # TODO add manual defioniton of comp and ext? like for __getCV
    # lastSegementSplits = lastSegment.split("|")[2]

    dotSplits = lastSegment.split("#")[0].rsplit(".",2)
    
    if len(dotSplits) > 1:
        formatExtension = dotSplits[-1]
        extensionPart += "."+formatExtension  
    
    if len(dotSplits) > 2:
        compression = dotSplits[-2]  
        extensionPart += "."+compression
    
    return (extensionPart, formatExtension, compression)


def __getFileInfo(artifactName, url):

    contentVariantPart = __getCV(url)
    extensionPart, formatExtension, compression = __getExtensions(url)

    if debug:
        print("DEBUG",url, extensionPart)

    name =f"{artifactName}{contentVariantPart}{extensionPart}"

    __url = str(url).split("|")[0]
    resp = requests.get(__url)
    if resp.status_code > 400:
        print(f"ERROR for {__url} -> Status {str(resp.status_code)}")

    sha256sum = hashlib.sha256(bytes(resp.content)).hexdigest()
    contentLength = len(resp.content)

    return (name, formatExtension, compression, contentLength, sha256sum)


def createDataset(versionId, title, abstract, description, license, downloadUrls):

    _versionId = str(versionId).strip("/")
    _, accountName, groupName, artifactName, version = _versionId.rsplit("/",4)

    # could be build from stuff above, 
    # was not sure if there are edge cases BASE=http://databus.example.org/"base"/...
    groupId = _versionId.rsplit("/",2)[0]

    distribution = []
    for url in downloadUrls:

        __url = str(url).split("|")[0]
        (name, formatExtension, compression, contentLength, sha256sum) = __getFileInfo(artifactName,url)

        entity = {
            "@id": f"{_versionId}#{name}",
            "@type": "Part",
            "file": f"{_versionId}/{name}",
            "formatExtension": formatExtension,
            "compression": compression,
            "downloadURL": __url,
            "byteSize": contentLength,
            "sha256sum": sha256sum
        }
        distribution.append(entity)

    dataset = {
        "@context" : "https://downloads.dbpedia.org/databus/context.jsonld",
        "@graph" : [
            {        
            "@id" : groupId,
            "@type": "Group",
            "title": title,
            "abstract": abstract,
            "description": description
            },
            {
            "@type": "Dataset",
            "@id": f"{_versionId}#Dataset",
            "hasVersion": version,
            "title": title,
            "abstract": abstract,
            "description": description,
            "license": license,
            "distribution": distribution
            }
        ]
    }
    return dataset


def deploy(dataid, api_key):
    print(dataid)
    headers = {
      "X-API-KEY": f"{api_key}",
      "Content-Type": "application/json"
    }
    data = json.dumps(dataid)

    resp = requests.post('https://d8lr.tools.dbpedia.org/api/publish', data = data, headers = headers)
    print(resp.status_code)
    if debug:
        print("---")
        print(resp.content)


# dataset = createDataset(
#     "https://d8lr.tools.dbpedia.org/hopver/testGroup/testArtifact/1.0-alpha/", 
#     "Test Title", "Test Abstract", "Test Description", 
#     "http://dalicc.net/licenselibrary/AdaptivePublicLicense10", 
#     ["https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml"]
# )

# apikey = "369df3c4-a6f1-49d0-a49c-c670bb558dfc"

# # print(json.dumps(dataset))
# # print("---")
# # deploy(dataset, apikey)

# import pandas as pd

# sheet_id = "1j72p6HUGGUh3peduictrhjDqo3JaJLFbVs7-4aywHHc"
# sheet_name = "relevant-datasets"
# # url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
# # url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?exportFormat&csvsheet={sheet_name}"
# url = "https://docs.google.com/spreadsheets/d/1j72p6HUGGUh3peduictrhjDqo3JaJLFbVs7-4aywHHc/export?exportFormat=csv&gid=1589516383"

# df = pd.read_csv(url, sep=",")
# df.reset_index

# for idx, row in df.iterrows():

#     title = row["atifact_label"]
#     artifactName = row['atifact_id']
#     version = row['version']
#     abstract = title
#     description = row['ducumentation']
#     contentVariants  = str(row['content_variant']).replace(";","_")

#     print("DEBUG", title)

#     license = row['license']

#     url = row['url']

#     dataset = createDataset(
#         f"https://d8lr.tools.dbpedia.org/hopver/llod/{artifactName}/{version}",
#         title, abstract, description,
#         license,
#         [url+"|"+contentVariants]   
#     )

#     print(json.dumps(dataset))#, indent=2, sort_keys=True))
#     print("---")
#     deploy(dataset,apikey)
#     print()
#     print()
#     print()
