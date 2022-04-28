# Quickstart

## DBpedia Databus Client 

### Installation
```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

### Execution example
```
bin/DatabusClient -s ./src/resources/queries/example.sparql -f jsonld -c gz
```
You will find more information if you set the flag`-h` or in [CLI usage](cli.md).

## Separate downloader or converter

The converter and downloader of the Databus Client can be used separately.

### Databus based downloader

Since the parameters `compression` and `format` both have the default value `equal`, the Databus Client is a pure downloader if you do not pass any arguments for compression and format.
```
bin/DatabusClient -s ./src/resources/queries/example.sparql
```

### Compression and format converter

If you select already existing files as the `source`, the client does not use the download module and behaves like a pure converter.
```
bin/DatabusClient -s ./src/test/resources/databus-client-testbed/format-testbed/2019.08.30/ -f ttl -c gz
```