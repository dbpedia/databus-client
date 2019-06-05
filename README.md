DBpedia-Client
----------------------------------------

1. Open Terminal
2. docker build -t dbpedia-client .
3. docker run -p 8890:8890 --name dbpediaclient dbpedia-client


You need to manipulate the run.sh file to change the program variables.

There you can change all the variables that are shown in Example2. The only difference is that you have to delimit the variables with pipe-symbols.

Example (run.sh):
mvn scala:run -Dlauncher=execute -DaddArgs="-q|/root/dbpediaclient/src/query/query|--compression|gz|"

4. You can load any ?file query. You have the choice either to pass the query directly as a program variable (-q), or save a query in a file and pass the filepath as variable



Example2 (pass the program variable --help to get following information): 
-----------------------------------------
Example: scala main.scala -q ./src/query/downloadquery  --repo converted_files/ --compression gz -f ttl

For usage see below:
    
  -c, --compression  <arg>   set the compressionformat of the outputfile
  -f, --format  <arg>        set the fileformat of the outputfile
  -q, --query  <arg>         any ?file query; You can pass the query directly or save it in a textfile and pass the filepath
  -r, --repo  <arg>          set the destination directory for converted files
      --help                 Show this message
      

All program variables have default values:
query: the query by default is loaded from the "./src/query/query" file
repo: "./converted_files/"
format: "ttl"   <-- format conversion isn't working yet
compression: "bz2"



4. docker stop dbpediaclient
