from typing import List, Dict, Tuple, Optional
import requests
import hashlib
import json

__debug = False

def __getCV(distribution_str: str) -> str:
    
    url_arguments = distribution_str.split("|")

    # cv string is ALWAYS at position 1 after the URL
    cv_str = url_arguments[1].strip("_")

    return f"_{cv_str}"

def __getFiletypeDefinition(distribution_str: str) -> Tuple[Optional[str], Optional[str]]:

    file_ext = None
    compression = None

    # take everything except URL
    metadata_list = distribution_str.split("|")[1:]

    
    if len(metadata_list) == 3:
        # compression and format
        file_ext = metadata_list[-2]
        compression = metadata_list[-1]
    elif len(metadata_list) == 2:
        # only format -> compression is None
        file_ext = metadata_list[-1]
        compression = None
    elif len(metadata_list) == 1:
        # let them be None to be later inferred from URL path
        pass
    else:
        # in any other case: unreadable arguments
        raise ValueError(f"Cant read the arguments {metadata_list}: Only takes 1-3 elements in arguments after the URL [CVs, format, compression]")
    
    return file_ext, compression


def __getExtensions(distribution_str: str) -> Tuple[str, str, str]:

    extensionPart = ""
    formatExtension, compression = __getFiletypeDefinition(distribution_str)

    if formatExtension is not None:
        # build the format extension (only append compression if not none)
        extensionPart = f".{formatExtension}"
        if compression is not None:
            extensionPart += f".{compression}"
        else:
            compression = "none"
        return (extensionPart, formatExtension, compression)

    # here we go if format not explicitly set: infer it from the path

    # first set default values
    formatExtension = "file"
    compression = "none"

    # get the last segment of the URL
    lastSegment = str(distribution_str).split("|")[0].split("/")[-1]

    # cut of fragments and split by dots
    dotSplits = lastSegment.split("#")[0].rsplit(".",2)
    
    if len(dotSplits) > 1:
        # if only format is given (no compression)
        formatExtension = dotSplits[-1]
        extensionPart = f".{formatExtension}"
    
    if len(dotSplits) > 2:
        # if format and compression is in the filename
        compression = dotSplits[-1]
        formatExtension = dotSplits[-2]  
        extensionPart = f".{formatExtension}.{compression}"
    
    return (extensionPart, formatExtension, compression)


def __getFileInfo(artifactName: str, distribution_str: str) -> Tuple[str, str, str, int, str]:

    contentVariantPart = __getCV(distribution_str)
    extensionPart, formatExtension, compression = __getExtensions(distribution_str)

    if __debug:
        print("DEBUG", distribution_str, extensionPart)

    name =f"{artifactName}{contentVariantPart}{extensionPart}"

    __url = str(distribution_str).split("|")[0]
    resp = requests.get(__url)
    if resp.status_code > 400:
        print(f"ERROR for {__url} -> Status {str(resp.status_code)}")

    sha256sum = hashlib.sha256(bytes(resp.content)).hexdigest()
    contentLength = len(resp.content)

    return (name, formatExtension, compression, contentLength, sha256sum)


def create_distribution(url: str, cvs: Dict[str, str], file_format: str=None, compression: str=None) -> str:
    """Creates the the identifier-string for a distribution used as downloadURLs in the createDataset function.
    url: is the URL of the dataset
    cvs: dict of content variants identifying a certain distribution (needs to be unique for each distribution in the dataset)
    file_format: identifier for the file format (e.g. json). If set to None client tries to infer it from the path
    compression: identifier for the compression format (e.g. gzip). If set to None client tries to infer it from the path   
    """

    meta_string = "_".join([f"{key}={value}" for key, value in cvs.items()])

    # check wether to add the custom file format
    if file_format is not None: 
        meta_string += f"|{file_format}"

    # check wether to addd the custom compression string
    if compression is not None: 
        meta_string += f"|{compression}"

    return f"{url}|{meta_string}"

def createDataset(versionId: str, title: str, abstract: str, description: str, license: str, distributions: List[str], group_title: str=None, group_abstract: str=None, group_description: str=None) -> Dict:

    _versionId = str(versionId).strip("/")
    _, accountName, groupName, artifactName, version = _versionId.rsplit("/",4)

    # could be build from stuff above, 
    # was not sure if there are edge cases BASE=http://databus.example.org/"base"/...
    groupId = _versionId.rsplit("/",2)[0]

    distribution = []
    for dst_string in distributions:

        __url = str(dst_string).split("|")[0]
        (name, formatExtension, compression, contentLength, sha256sum) = __getFileInfo(artifactName,dst_string)

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

    group_dict = {
        "@id" : groupId,
        "@type": "Group",
    }

    # add group metadata if set, else it can be left out
    for k, val in [("title", group_title), ("abstract", group_abstract), ("description", group_description)]:
        if val is not None:
            group_dict[k] = val

    dataset = {
        "@context" : "https://downloads.dbpedia.org/databus/context.jsonld",
        "@graph" : [
            group_dict,
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

    base = "/".join(dataid['@graph'][0]['@id'].split('/')[0:3])+"/api/publish"

    resp = requests.post(base, data = data, headers = headers)
    print(resp.status_code)
    if __debug:
        print("---")
        print(resp.content)


if __name__ == "__main__":
    print("empty)")
    
