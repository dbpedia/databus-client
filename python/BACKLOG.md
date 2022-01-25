# BACKLOG

* gen preview for files
* add shell completion script


## Current Problems

### Redeploy

Currently there are problems handling multiple `key,value` pairs of content variants, its only working by excluding the multiple ones in the query.

```
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
```

## CLI Problems

Currently there is no syntax for submitting format extensions or content variants etc to the CLI for Databus Versions. A Sntax needs to be set, for example:
```
python3 -m databusclient [...] "http://akswnc7.informatik.uni-leipzig.de/dstreitmatter/archivo/advene.org/ns--cinelab--ld/2020.06.10-175249/ns--cinelab--ld_type=generatedDocu.html|key1=value1|value2|.format|..compression"
```
Something like this, idk.