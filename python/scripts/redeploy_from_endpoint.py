from SPARQLWrapper import SPARQLWrapper, JSON
from databusclient.databus_client import DatabusFile, DatabusGroup, DatabusVersionMetadata, DatabusVersion, deploy_to_dev_databus
from datetime import datetime

endpoint = "https://databus.dbpedia.org/repo/sparql"

db_base = "http://localhost:3000"

def redeploy_groups():
  
  query = """
  PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
  PREFIX dct:    <http://purl.org/dc/terms/>
  PREFIX dcat:   <http://www.w3.org/ns/dcat#>
  PREFIX db:     <https://databus.dbpedia.org/>
  PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX databus: <https://databus.dbpedia.org/>

  SELECT DISTINCT ?group ?groupdocu WHERE {
    ?dataset dataid:account databus:ontologies .
    ?dataset dataid:group ?group .
    ?dataset dataid:groupdocu ?groupdocu .
  } 
  """


  sparql_wrapper = SPARQLWrapper(endpoint)
  sparql_wrapper.setReturnFormat(JSON)

  sparql_wrapper.setQuery(query)

  result = sparql_wrapper.queryAndConvert()

  all_groups = []

  for binding in result["results"]["bindings"]:

    group_uri = binding["group"]["value"]
    group = group_uri.split("/")[-1]

    docu = binding["groupdocu"]["value"]

    all_groups.append(DatabusGroup("denis", group, f"Title for {group_uri}", f"Label for {group_uri}", f"This is the comment for {group_uri}", f"This is the absrtact for {group_uri}", docu, DATABUS_BASE=db_base))

  deploy_to_dev_databus("ff7d6b48-86b8-4760-ad02-9ef5de2608d9", *all_groups)

def redeploy_versions():

  

  query = """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX databus: <https://databus.dbpedia.org/>
PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
PREFIX dataid-cv: <http://dataid.dbpedia.org/ns/cv#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?group ?art ?version ?title ?publisher ?comment ?description ?license ?file ?extension ?type ?bytes ?shasum WHERE { 
  ?dataset dataid:account databus:ontologies .
  ?dataset dataid:group ?group .
  ?dataset dataid:artifact ?art.
  ?dataset dcat:distribution ?distribution .
  ?dataset dct:license ?license .
  ?dataset dct:publisher ?publisher .
  ?dataset rdfs:comment ?comment .
  ?dataset dct:description ?description .
  ?dataset dct:title ?title .
  ?distribution dcat:downloadURL ?file .
  ?distribution dataid:formatExtension ?extension .
  ?distribution dataid-cv:type ?type .
  ?distribution dcat:byteSize ?bytes .
  ?distribution dataid:sha256sum ?shasum .
  ?dataset dct:hasVersion ?version .
  # Excludes dev versions
  FILTER (!regex(?art, "--DEV"))
  # exclude some stuff since content variants are hard
  MINUS { ?distribution dataid:contentVariant 'sorted'^^xsd:string . }
  MINUS { ?distribution dataid:contentVariant 'NONE'^^xsd:string}
  MINUS { ?distribution dataid:contentVariant 'goodLicense'^^xsd:string}
  MINUS { ?distribution dataid:contentVariant 'lodeMetadata'^^xsd:string}
  MINUS { ?distribution dataid:contentVariant 'old'^^xsd:string}
} ORDER BY ?version
  """

  sparql_wrapper = SPARQLWrapper(endpoint)
  sparql_wrapper.setReturnFormat(JSON)

  sparql_wrapper.setQuery(query)

  result = sparql_wrapper.queryAndConvert()

  result_map = {}

  issued = datetime.now()

  for binding in result["results"]["bindings"]:

    
      
    group = binding["group"]["value"].split("/")[-1]
    art = binding["art"]["value"].split("/")[-1]
    version = binding["version"]["value"]
    dl_url = binding["file"]["value"]
    file_ext = binding["extension"]["value"]
    # set fileext for unknown files
    if file_ext == "":
      file_ext = "file"
    file_type = binding["type"]["value"]
    bytesize = binding["bytes"]["value"]
    shasum = binding["shasum"]["value"]

    publisher = "http://localhost:3000/denis#this"
    title = binding["title"]["value"]
    comment = binding["comment"]["value"]
    description = binding["description"]["value"]
    version_license = binding["license"]["value"]

    version_metadata = DatabusVersionMetadata("denis", group, art, version, title, title, publisher, comment, description, description, version_license, issued=issued, DATABUS_BASE=db_base)


    databus_files = result_map.get(version_metadata, []) + [DatabusFile(dl_url, {"type": file_type}, file_ext, shasum=shasum, content_length=bytesize)]
    result_map[version_metadata] = databus_files

  versions = []

  for metadata, dbfiles in result_map.items():
    versions.append(DatabusVersion(metadata, dbfiles))

  # for version in versions:
  #   print(version.metadata.group, version.metadata.artifact, version.metadata.version, len(version.databus_files))

  deploy_to_dev_databus("ff7d6b48-86b8-4760-ad02-9ef5de2608d9", *versions)


def main():

  redeploy_versions()

if __name__ == "__main__":
  main()