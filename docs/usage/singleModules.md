# Single Modules

You can also use the converter and downloader separately.

### Databus based Downloader

* Due default values of `compression` and `format` are `same`, the Client is a pure downloader, if you don't pass arguments for `compression` and `format`.
```
bin/DatabusClient -s ./src/resources/queries/example.sparql -t ./downloaded_files/
```

### Compression and Format Converter

* If you choose already existing files as source, the client doesn't use the download module and behaves like a pure converter.
```
bin/DatabusClient --source ./src/test/resources/databus-client-testbed/format-testbed/2019.08.30/ -t ./converted_files/ -f ttl -c gz
```