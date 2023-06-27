# Download data using Collection Uri

The easiest way to use the client is to pass a CollectionIRI as a source parameter. This has two advantages. First, the user interface of the collection editor is available and you do not need to write Sparql queries anymore. On the other hand you don't need to pass the `endpoint` parameter to the client anymore, because the client recognizes by itself where the collection is stored.

## Example

```
java -jar databus-client.jar -s link/to/your/collection
```
