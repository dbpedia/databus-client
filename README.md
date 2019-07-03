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

**Important: If you use docker to execute, you can't change the "_REPO_" yet.**
**Important: At the moment only conversion to ntriples(_"nt"_) or _"same"_ possible**

To stop the image *client* in the container *dbpedia-client* use `docker stop client`

***
#### Example(Terminal)   

1. ```scalac main.scala```    
2. ```scala main -q ./src/query/downloadquery -r converted_files/ -c gz -f ttl```

***

##### Program Variables Overview
Pass the program variable `--help` to get the following information:  

For usage see below:  
    
  -c, --compression  <arg>  &emsp;set the compressionformat of the outputfile  
  -f, --format  <arg>       &emsp;set the fileformat of the outputfile  
  -q, --query  <arg>        &emsp;any ?file query; You can pass the query directly or save it in a textfile and pass the filepath  
  -r, --repo  <arg>         &emsp;set the destination directory for converted files  
      --help                &emsp;Show this message  
      
***

##### Default Values
All program variables have default values:  
*query: "./src/query/query" &emsp;<- the query is loaded from this file by default  
repo: "./converted_files/"  
format: "same"  
compression: "same"*  


