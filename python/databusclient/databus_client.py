import requests
import json
import hashlib
import sys
from datetime import datetime
from dataclasses import dataclass, field
from typing import List
import argparse_prompt
import logging


@dataclass
class DatabusGroup:
    account_name: str
    id: str
    label: str
    title: str
    comment: str
    abstract: str
    description: str
    DATABUS_BASE: str = "https://dev.databus.dbpedia.org"
    context: str = "https://raw.githubusercontent.com/dbpedia/databus-git-mockup/main/dev/context.jsonld"

    def get_target_uri(self) -> str:

        return f"{self.DATABUS_BASE}/{self.account_name}/{self.id}"

    def to_jsonld(self, **kwargs) -> str:
        """Generates the json representation of group documentation"""

        group_uri = f"{self.DATABUS_BASE}/{self.account_name}/{self.id}"

        group_data_dict = {
            "@context": self.context,
            "@graph": [
                {
                    "@id": group_uri,
                    "@type": "dataid:Group",
                    "label": {"@value": self.label, "@language": "en"},
                    "title": {"@value": self.title, "@language": "en"},
                    "comment": {"@value": self.comment, "@language": "en"},
                    "abstract": {"@value": self.abstract, "@language": "en"},
                    "description": {"@value": self.description, "@language": "en"},
                }
            ],
        }
        return json.dumps(group_data_dict, **kwargs)


class DatabusFile:
    def __init__(
        self,
        uri: str,
        cvs: dict,
        file_ext: str,
        verbose=False,
        shasum=None,
        content_length=None,
        **kwargs,
    ):
        """Fetches the necessary information of a file URI for the deploy to the databus."""
        self.uri = uri
        self.cvs = cvs

        if shasum is None or content_length is None:
            self.__fetch_file_info(uri, **kwargs)
        else:
            self.sha256sum = shasum
            self.content_length = content_length

        self.file_ext = file_ext
        self.id_string = "_".join([f"{k}={v}" for k, v in cvs.items()]) + "." + file_ext

    def __fetch_file_info(uri, **kwargs):
        resp = requests.get(uri, **kwargs)

        if verbose:
            print(f"")
        if resp.status_code > 400:
            print(f"ERROR for {uri} -> Status {str(resp.status_code)}")

        self.sha256sum = hashlib.sha256(bytes(resp.content)).hexdigest()
        self.content_length = str(len(resp.content))


@dataclass(eq=True, frozen=True)
class DatabusVersionMetadata:
    account_name: str
    group: str
    artifact: str
    version: str
    title: str
    label: str
    publisher: str
    comment: str
    abstract: str
    description: str
    license: str
    issued: datetime = field(default_factory=datetime.now)
    DATABUS_BASE: str = "https://dev.databus.dbpedia.org"
    context: str = "https://raw.githubusercontent.com/dbpedia/databus-git-mockup/main/dev/context.jsonld"


class DatabusVersion:
    def __init__(self, metadata, databus_files):
        self.metadata = metadata
        self.databus_files = databus_files

    def get_target_uri(self):

        return f"{self.metadata.DATABUS_BASE}/{self.metadata.account_name}/{self.metadata.group}/{self.metadata.artifact}/{self.metadata.version}"

    def __distinct_cvs(self) -> dict:

        distinct_cv_definitions = {}
        for dbfile in self.databus_files:
            for key, value in dbfile.cvs.items():

                if not key in distinct_cv_definitions:
                    distinct_cv_definitions[key] = {
                        "@type": "rdf:Property",
                        "@id": f"dataid-cv:{key}",
                        "rdfs:subPropertyOf": {"@id": "dataid:contentVariant"},
                    }
        return distinct_cv_definitions

    def __dbfiles_to_dict(self):

        for dbfile in self.databus_files:
            file_dst = {
                "@id": self.version_uri + "#" + dbfile.id_string,
                "file": self.version_uri + "/" + self.metadata.artifact + "_" + dbfile.id_string,
                "@type": "dataid:SingleFile",
                "formatExtension": dbfile.file_ext,
                "compression": "none",
                "downloadURL": dbfile.uri,
                "byteSize": dbfile.content_length,
                "sha256sum": dbfile.sha256sum,
                "hasVersion": self.metadata.version,
            }
            for key, value in dbfile.cvs.items():

                file_dst[f"dataid-cv:{key}"] = value

            yield file_dst

    def to_jsonld(self, **kwargs) -> str:
        self.version_uri = f"{self.metadata.DATABUS_BASE}/{self.metadata.account_name}/{self.metadata.group}/{self.metadata.artifact}/{self.metadata.version}"
        self.data_id_uri = self.version_uri + "#Dataset"

        self.artifact_uri = f"{self.metadata.DATABUS_BASE}/{self.metadata.account_name}/{self.metadata.group}/{self.metadata.artifact}"

        self.group_uri = (
            f"{self.metadata.DATABUS_BASE}/{self.metadata.account_name}/{self.metadata.group}"
        )

        self.timestamp = self.metadata.issued.strftime("%Y-%m-%dT%H:%M:%SZ")

        data_id_dict = {
            "@context": self.metadata.context,
            "@graph": [
                {
                    "@type": "dataid:Dataset",
                    "@id": self.data_id_uri,
                    "version": self.version_uri,
                    "artifact": self.artifact_uri,
                    "group": self.group_uri,
                    "hasVersion": self.metadata.version,
                    "issued": self.timestamp,
                    "publisher": self.metadata.publisher,
                    "label": {"@value": self.metadata.label, "@language": "en"},
                    "title": {"@value": self.metadata.title, "@language": "en"},
                    "comment": {"@value": self.metadata.comment, "@language": "en"},
                    "abstract": {"@value": self.metadata.abstract, "@language": "en"},
                    "description": {
                        "@value": self.metadata.description,
                        "@language": "en",
                    },
                    "license": {"@id": self.metadata.license},
                    "distribution": [d for d in self.__dbfiles_to_dict()],
                }
            ],
        }

        for _, named_cv_prop in self.__distinct_cvs().items():
            data_id_dict["@graph"].append(named_cv_prop)

        # add explicit artifact statement

        data_id_dict["@graph"].append(
            {"@id": self.get_target_uri().rsplit("/", 1)[0], "@type": "dataid:Artifact"}
        )

        # Explicit Version Statement
        data_id_dict["@graph"].append(
            {"@id": self.get_target_uri(), "@type": "dataid:Version"}
        )

        return json.dumps(data_id_dict)


def deploy_to_dev_databus(api_key: str, *databus_objects):

    for i, dbobj in enumerate(databus_objects):
        print(f"{i}: Deploying {dbobj.get_target_uri()}")
        submission_data = dbobj.to_jsonld()

        resp = requests.put(
            dbobj.get_target_uri(),
            headers={"X-API-Key": api_key, "Content-Type": "application/json"},
            data=submission_data,
        )

        if resp.status_code >= 400:
            print(f"Response: Status {resp.status_code}; Text: {resp.text}")

            print(f"Problematic file:\n {submission_data}")
