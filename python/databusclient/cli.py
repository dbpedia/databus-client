from databusclient import databus_client
import argparse_prompt
from urllib.parse import urlparse
import re

def parse_cv_string(s: str, seperator: str="|"):

    result_map = {}
    for cv in s.split(seperator):
        key, value = cv.split("=")
        result_map[key] = value
    return result_map
        
def generate_databus_file(arg):

    uri = arg.split("|")[0]

    # TODO make it possible to pass file extension instead of autofetching
    file_ext = None

    cv_string = arg[arg.find("|")+1:]

    cv_map = parse_cv_string(cv_string)

    return databus_client.DatabusFile(uri, cv_map, file_ext)

def direct_group_deploy(args):

    group = databus_client.DatabusGroup(
        account_name=args.user,
        id=args.group,
        title=args.title,
        abstract=args.abstract,
        description=args.doc,
        DATABUS_BASE=args.base,
    )

    databus_client.deploy_to_dev_databus(args.apikey, group)


def generate_group(args):

    group = databus_client.DatabusGroup(
        account_name=args.user,
        id=args.group,
        title=args.title,
        abstract=args.abstract,
        description=args.doc,
        DATABUS_BASE=args.base,
    )

    if args.file is not None and args.file != "":
        fname = args.file
    else:
        fname = "group.jsonld"

    with open(fname, "w+") as f:
        print(group.to_jsonld(indent=2), file=f)


def generate_version(args):

    dbfiles = []

    for arg_string in args.URIs:
        dbfiles.append(generate_databus_file(arg_string))

    version_metadata = databus_client.DatabusVersionMetadata(
        account_name=args.user,
        group=args.group,
        artifact=args.artifact,
        version=args.versionid,
        title=args.title,
        publisher=args.publisher,
        abstract=args.abstract,
        description=args.doc,
        license=args.license,
        DATABUS_BASE=args.base,
    )

    version = databus_client.DatabusVersion(
        version_metadata,
        dbfiles,
    )
    if args.file is not None and args.file != "":
        fname = args.file
    else:
        fname = "version.jsonld"

    with open(fname, "w+") as f:
        print(version.to_jsonld(indent=2), file=f)


def direct_version_deploy(args):

    dbfiles = []

    for arg_string in args.URIs:
        dbfiles.append(generate_databus_file(arg_string))

    version_metadata = databus_client.DatabusVersionMetadata(
        account_name=args.user,
        group=args.group,
        artifact=args.artifact,
        version=args.versionid,
        title=args.title,
        publisher=args.publisher,
        abstract=args.abstract,
        description=args.doc,
        license=args.license,
        DATABUS_BASE=args.base,
    )

    version = databus_client.DatabusVersion(
        version_metadata,
        dbfiles,
    )

    databus_client.deploy_to_dev_databus(args.apikey, version)


def main():

    parser = argparse_prompt.PromptParser()

    subparsers = parser.add_subparsers(dest="command", help="sub-command help")

    # Some parameters you always need

    parser.add_argument(
        "--verbose",
        "-v",
        help="Prints out steps and generated content",
        action="store_true",
        prompt=False,
    )

    parser.add_argument(
        "--base",
        "-b",
        help="The base for the Databus. Default is https://dev.databus.dbpedia.org",
        default="https://dev.databus.dbpedia.org",
        prompt=False,
    )

    # first do the generate parsers for both groups and Versions
    generate_parser = subparsers.add_parser("generate", help="generate help")

    generate_subparsers = generate_parser.add_subparsers(dest="type", help="type help")

    generate_parser.add_argument(
        "--file", "-f", help="The file the result should be printed to.", type=str
    )

    # parser for generating group

    group_generate_parser = generate_subparsers.add_parser("group", help="group help")

    group_generate_parser.add_argument(
        "--user", "-u", help="The databus user", type=str
    )

    group_generate_parser.add_argument("--group", "-g", help="The group name", type=str)

    group_generate_parser.add_argument(
        "--title", "-t", help="The group title", type=str
    )

    group_generate_parser.add_argument(
        "--abstract", "-a", help="The group abstract", type=str
    )

    group_generate_parser.add_argument(
        "--doc", "-doc", help="The group documentation", type=str
    )

    group_generate_parser.add_argument(
        "--file", "-f", help="The file the result should be printed to.", type=str
    )

    # parser for generating version

    version_generate_parser = generate_subparsers.add_parser(
        "version", help="version help"
    )

    version_generate_parser.add_argument(
        "--user", "-u", help="The databus user", type=str
    )

    version_generate_parser.add_argument("--group", help="The group name", type=str)

    version_generate_parser.add_argument(
        "--artifact", help="The version artifact", type=str
    )

    version_generate_parser.add_argument("--versionid", help="The version id", type=str)

    version_generate_parser.add_argument("--title", help="The version title", type=str)

    version_generate_parser.add_argument(
        "--publisher", help="The version publisher", type=str
    )

    version_generate_parser.add_argument(
        "--abstract", "-a", help="The abstract of the artifact", type=str
    )

    version_generate_parser.add_argument(
        "--doc", help="The version documentation", type=str
    )

    version_generate_parser.add_argument(
        "--license", help="The version license", type=str
    )

    version_generate_parser.add_argument(
        "--file", "-f", help="The file the result should be printed to.", type=str
    )

    version_generate_parser.add_argument(
        "URIs", nargs="+", help="All the uris to be deployed to the Databus", type=str
    )

    # The parsers for direct deploy

    direct_deploy_parser = subparsers.add_parser("deploy", help="deploy help")

    direct_deploy_subparsers = direct_deploy_parser.add_subparsers(
        dest="type", help="deploy group help"
    )

    direct_deploy_parser.add_argument(
        "--apikey", help="Set the API key from your Databus Account", secure=True
    )

    # parser for deploying the group directly

    direct_group_deploy_parser = direct_deploy_subparsers.add_parser("group")

    direct_group_deploy_parser.add_argument("--user", help="The databus user", type=str)

    direct_group_deploy_parser.add_argument("--group", help="The group name", type=str)

    direct_group_deploy_parser.add_argument("--title", help="The group title", type=str)

    direct_group_deploy_parser.add_argument(
        "--abstract", "-a", help="The group abstract", type=str
    )

    direct_group_deploy_parser.add_argument(
        "--doc", help="The group documentation", type=str
    )

    # parsers for deploying the version directly

    direct_version_deploy_parser = direct_deploy_subparsers.add_parser("version")

    direct_version_deploy_parser.add_argument(
        "--user", help="The databus user", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--group", help="The group name", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--artifact", help="The version artifact", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--versionid", help="The version id", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--title", help="The version title", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--publisher", help="The version publisher", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--abstract", help="The artifacts abstract", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--doc", help="The version documentation", type=str
    )

    direct_version_deploy_parser.add_argument(
        "--license", help="The version license", type=str
    )

    direct_version_deploy_parser.add_argument(
        "URIs", nargs="+", help="URIs to publish", type=str
    )

    print("DATABUS CLIENT v0.1")
    print("-------------------")
    print("Please insert the missing metadata:")

    args = parser.parse_args()

    if args.command == "generate":

        if args.type == "group":
            generate_group(args)
        elif args.type == "version":
            generate_version(args)
        else:
            print(f"Couldn't recognize type {args.type}")
    elif args.command == "deploy":

        if args.type == "group":
            direct_group_deploy(args)
        elif args.type == "version":
            direct_version_deploy(args)
        else:
            print(f"Couldn't recognize type {args.type}")
    else:
        print(f"Couldn't recognize command {args.command}")


if __name__ == "__main__":
    main()
