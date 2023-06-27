# Scala/Java API

The Databus Client also offers an [API](../../src/main/scala/org/dbpedia/databus/client/api/DatabusClient.scala) for easy integration into your project. Currently i\
Currently there is no entry on mvn central for the Databus Client. But you can include the jar of the latest release in your project.

## Installation

#### download jar file

* Download the `databus-client.jar` of the latest [Databus Client release](https://github.com/dbpedia/databus-client/releases/latest)
* Move the file to `${project.basedir}/src/main/resources/databus-client.jar`

#### include dependency (example for maven)

```
<dependency>
    <groupId>com.sample</groupId>
    <artifactId>sample</artifactId>
    <version>1.0</version>
    <scope>system</scope>
    <systemPath>${project.basedir}/src/main/resources/databus-client.jar</systemPath>
</dependency>
```

### Execution

After you included the DatabusClient dependency, you can use it in your application.

```
DatabusClient
    .source("./src/query/query1")
    .source("./src/query/query2")
    .compression(Compression.gz)
    .format(Format.nt)
    .execute()
```

## Sample project (outdated)

We have also created a [sample project](https://github.com/dbpedia/databus-client/tree/master/examples/sample\_project) that shows how the Databus Client can be integrated into a project.
