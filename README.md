# DBpedia-Client
***

#### Example(Docker)

1. Open Terminal
2. `docker build -t dbpedia-client .`    <--(dont forget the dot)
3. `docker run -p 8890:8890 --name client -e QUERY=/root/dbpediaclient/src/query/query dbpedia-client`

You can pass all the variables that are shown in Example(Terminal) as Environment Variables (**-e**).  
You have to write the Environment Variables in Capital Letters, if you use docker to execute.  
Example: `docker run -p 8890:8890 --name client -e Q=<path> -e F=nt -e C=gz dbpedia-client`

+ You can load any ?file query. 
    - You have the choice either to pass the query directly as a program variable (`-e QUERY="..."`), or save a query in a file and pass the filepath as variable.
+ You can choose between different compression formats:
    - *"bz2"*
    - *"gz"*
    - *"br"*
    - *"snappy-framed", "snappy-raw"*  
    - *"deflate", "deflate64"*
    - *"lz4-block", "lz4-framed"*
    - *"lzma"*
    - *"pack200"*
    - *"xz"*
    - *"z"*
    - *"zstd"*  
    - additionally you can choose between *"same"* (no change of compression) or *"no"* for no compression.

**Important: If you use docker to execute, you can't change the "_TARGETREPO_" yet.**  
**Important: At the moment only conversion to NTriples(_"nt"_), TSV(_"tsv"_), Json-LD(_"jsonld"_) or _"same"_ possible**

To stop the image *client* in the container *dbpedia-client* use `docker stop client`

***
#### Examples(Terminal)   

##### Download and Convert
```mvn scala:run -Dlauncher=downloadconverter -q ./src/query/downloadquery --targetrepo converted_files/ -c gz -f jsonld```
##### Download only
```mvn scala:run -Dlauncher=downloader -q ./src/query/downloadquery -t ./downloaded_files/```
##### Convert only
```mvn scala:run -Dlauncher=converter --src ./downloaded_files/ -t ./converted_files/ -c gz -f jsonld```

***

##### Program Variables Overview
Pass the program variable `--help` to get the following information:  

For usage see below:  
    
  -c, --compression  <arg>  &emsp;set the compressionformat of the outputfile  
  -f, --format  <arg>       &emsp;set the fileformat of the outputfile  
  -q, --query  <arg>        &emsp;any ?file query; You can pass the query directly or save it in a textfile and pass the filepath  
  -t, --targetrepo  <arg>   &emsp;set the destination directory for converted files  
  -s, --src  <arg>          &emsp;set the source directory for files you want to convert
      --help                &emsp;Show this message  
      
***

##### Default Values
All program variables have default values:  
*query: "./src/query/query" &emsp;<- the query is loaded from this file by default  
targetrepo: "./converted_files/"  
src: "./tempdir_downloaded_files/"   
format: "same"  
compression: "same"*