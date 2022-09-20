from typing import List, Dict, Tuple, Optional
import requests
import hashlib
import json

__debug = False


class DeployError(Exception):
    """Raised if deploy fails"""


def __get_content_variants(distribution_str: str) -> Dict[str, str]:
    args = distribution_str.split("|")

    # cv string is ALWAYS at position 1 after the URL
    cv_str = args[1].strip("_")

    cvs = {}
    for kv in cv_str.split("_"):
        key, value = kv.split("=")
        cvs[key] = value

    return cvs


def __get_filetype_definition(distribution_str: str, ) -> Tuple[Optional[str], Optional[str]]:
    file_ext = None
    compression = None

    # take everything except URL
    metadata_list = distribution_str.split("|")[1:]

    if len(metadata_list) == 4:
        file_ext = metadata_list[-3]
        compression = metadata_list[-2]
    elif len(metadata_list) == 3:
        # when last item is shasum:length -> only file_ext set
        if ":" in metadata_list[-1]:
            file_ext = metadata_list[-2]
        else:
            # compression and format
            file_ext = metadata_list[-2]
            compression = metadata_list[-1]
    elif len(metadata_list) == 2:
        # if last argument is shasum:length -> both none
        if ":" in metadata_list[-1]:
            pass
        else:
            # only format -> compression is None
            file_ext = metadata_list[-1]
            compression = None

    elif len(metadata_list) == 1:
        # let them be None to be later inferred from URL path
        pass
    else:
        # in any other case: unreadable arguments
        raise ValueError(
            f"Cant read the arguments {metadata_list}: Only takes 1-4 elements in arguments after the URL [CVs, "
            f"format, compression, shasum:length]"
        )

    return file_ext, compression


def __get_extensions(distribution_str: str) -> Tuple[str, str, str]:
    extension_part = ""
    format_extension, compression = __get_filetype_definition(distribution_str)

    if format_extension is not None:
        # build the format extension (only append compression if not none)
        extension_part = f".{format_extension}"
        if compression is not None:
            extension_part += f".{compression}"
        else:
            compression = "none"
        return extension_part, format_extension, compression

    # here we go if format not explicitly set: infer it from the path

    # first set default values
    format_extension = "file"
    compression = "none"

    # get the last segment of the URL
    last_segment = str(distribution_str).split("|")[0].split("/")[-1]

    # cut of fragments and split by dots
    dot_splits = last_segment.split("#")[0].rsplit(".", 2)

    if len(dot_splits) > 1:
        # if only format is given (no compression)
        format_extension = dot_splits[-1]
        extension_part = f".{format_extension}"

    if len(dot_splits) > 2:
        # if format and compression is in the filename
        compression = dot_splits[-1]
        format_extension = dot_splits[-2]
        extension_part = f".{format_extension}.{compression}"

    return extension_part, format_extension, compression


def __get_file_stats(distribution_str: str) -> Tuple[Optional[str], Optional[int]]:
    metadata_list = distribution_str.split("|")[1:]
    # check whether there is the shasum:length tuple seperated by :
    if ":" not in metadata_list[-1]:
        return None, None

    last_arg_split = metadata_list[-1].split(":")

    if len(last_arg_split) != 2:
        raise ValueError(f"Can't parse Argument {metadata_list[-1]}. Too many values, submit shasum and "
                         f"content_length in the form of shasum:length")

    sha256sum = last_arg_split[0]
    content_length = int(last_arg_split[1])

    return sha256sum, content_length


def __load_file_stats(url: str) -> Tuple[str, int]:
    resp = requests.get(url)
    if resp.status_code > 400:
        raise requests.exceptions.RequestException(response=resp)

    sha256sum = hashlib.sha256(bytes(resp.content)).hexdigest()
    content_length = len(resp.content)
    return sha256sum, content_length


def __get_file_info(
        artifact_name: str, distribution_str: str
) -> Tuple[str, Dict[str, str], str, str, str, int]:
    cvs = __get_content_variants(distribution_str)
    extension_part, format_extension, compression = __get_extensions(distribution_str)

    content_variant_part = "_".join([f"{key}={value}" for key, value in cvs.items()])

    if __debug:
        print("DEBUG", distribution_str, extension_part)

    name = f"{artifact_name}_{content_variant_part}{extension_part}"

    sha256sum, content_length = __get_file_stats(distribution_str)

    if sha256sum is None or content_length is None:
        __url = str(distribution_str).split("|")[0]
        sha256sum, content_length = __load_file_stats(__url)

    return name, cvs, format_extension, compression, sha256sum, content_length


def create_distribution(
        url: str, cvs: Dict[str, str], file_format: str = None, compression: str = None,
        sha256_length_tuple: Tuple[str, int] = None
) -> str:
    """Creates the identifier-string for a distribution used as downloadURLs in the createDataset function.
    url: is the URL of the dataset
    cvs: dict of content variants identifying a certain distribution (needs to be unique for each distribution in the dataset)
    file_format: identifier for the file format (e.g. json). If set to None client tries to infer it from the path
    compression: identifier for the compression format (e.g. gzip). If set to None client tries to infer it from the path
    sha256_length_tuple: sha256sum and content_length of the file in the form of Tuple[shasum, length].
    If left out file will be downloaded extra and calculated.
    """

    meta_string = "_".join([f"{key}={value}" for key, value in cvs.items()])

    # check whether to add the custom file format
    if file_format is not None:
        meta_string += f"|{file_format}"

    # check whether to add the custom compression string
    if compression is not None:
        meta_string += f"|{compression}"

    # add shasum and length if present
    if sha256_length_tuple is not None:
        sha256sum, content_length = sha256_length_tuple
        meta_string += f"|{sha256sum}:{content_length}"

    return f"{url}|{meta_string}"


def createDataset(
        versionId: str,
        title: str,
        abstract: str,
        description: str,
        license: str,
        distributions: List[str],
        group_title: str = None,
        group_abstract: str = None,
        group_description: str = None,
) -> Dict:
    _versionId = str(versionId).strip("/")
    _, accountName, groupName, artifactName, version = _versionId.rsplit("/", 4)

    # could be build from stuff above,
    # was not sure if there are edge cases BASE=http://databus.example.org/"base"/...
    groupId = _versionId.rsplit("/", 2)[0]

    distribution = []
    for dst_string in distributions:
        __url = str(dst_string).split("|")[0]
        (
            name,
            cvs,
            formatExtension,
            compression,
            sha256sum,
            content_length,
        ) = __get_file_info(artifactName, dst_string)

        entity = {
            "@id": f"{_versionId}#{name}",
            "@type": "Part",
            "file": f"{_versionId}/{name}",
            "formatExtension": formatExtension,
            "compression": compression,
            "downloadURL": __url,
            "byteSize": content_length,
            "sha256sum": sha256sum,
        }
        # set content variants
        for key, value in cvs.items():
            entity[f"dcv:{key}"] = value

        distribution.append(entity)

    group_dict = {
        "@id": groupId,
        "@type": "Group",
    }

    # add group metadata if set, else it can be left out
    for k, val in [
        ("title", group_title),
        ("abstract", group_abstract),
        ("description", group_description),
    ]:
        if val is not None:
            group_dict[k] = val

    dataset = {
        "@context": "https://downloads.dbpedia.org/databus/context.jsonld",
        "@graph": [
            group_dict,
            {
                "@type": "Dataset",
                "@id": f"{_versionId}#Dataset",
                "hasVersion": version,
                "title": title,
                "abstract": abstract,
                "description": description,
                "license": license,
                "distribution": distribution,
            },
        ],
    }
    return dataset


def deploy(dataid, api_key):
    headers = {"X-API-KEY": f"{api_key}", "Content-Type": "application/json"}
    data = json.dumps(dataid)
    base = "/".join(dataid["@graph"][0]["@id"].split("/")[0:3]) + "/api/publish"
    resp = requests.post(base, data=data, headers=headers)

    if resp.status_code != 200:
        raise DeployError(f"Could not deploy dataset to databus. Reason: '{resp.text}'")
    if __debug:
        print("---")
        print(resp.content)


if __name__ == "__main__":
    print("empty)")
