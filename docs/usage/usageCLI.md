## CLI

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

____________________

## Jar

You can also use the released [databus-client.jar](https://github.com/dbpedia/databus-client/releases/latest), instead of cloning the whole repository

### Execution example
```
java -jar databus-client-1.0-SNAPSHOT.jar -s "https://databus.dbpedia.org/rogargon/collections/browsable_core"
```