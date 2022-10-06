"""Client tests"""
import pytest
from databusclient.client import create_dataset, create_distribution, __get_file_info
from collections import OrderedDict


def test_distribution_cases():

    metadata_args_with_filler = OrderedDict()

    metadata_args_with_filler["type=config_source=databus"] = ""
    metadata_args_with_filler["yml"] = None
    metadata_args_with_filler["none"] = None
    metadata_args_with_filler["79582a2a7712c0ce78a74bb55b253dc2064931364cf9c17c827370edf9b7e4f1:56737"] = None

    # test by leaving out an argument each
    artifact_name = "databusclient-pytest"
    uri = "https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml"
    parameters = list(metadata_args_with_filler.keys())

    for i in range(0, len(metadata_args_with_filler.keys())):

        if i == 1:
            continue

        dst_string = f"{uri}"
        for j in range(0, len(metadata_args_with_filler.keys())):
            if j == i:
                replacement = metadata_args_with_filler[parameters[j]]
                if replacement is None:
                    pass
                else:
                    dst_string += f"|{replacement}"
            else:
                dst_string += f"|{parameters[j]}"

        print(f"{dst_string=}")
        (
            name,
            cvs,
            formatExtension,
            compression,
            sha256sum,
            content_length,
        )= __get_file_info(artifact_name, dst_string)

        created_dst_str = create_distribution(uri, cvs, formatExtension, compression, (sha256sum, content_length))

        assert dst_string == created_dst_str
