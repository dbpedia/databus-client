DBpedia-Client
----------------------------------------

Example(Docker)

1. Open Terminal
2. '''docker build -t dbpedia-client .'''
3. docker run -p 8890:8890 --name client -e QUERY=/root/dbpediaclient/src/query/query dbpedia-client

You can pass all the variables that are shown in Example(Terminal) as Environment Variables (-e).
You have to write the Environment Variables in Capital Letters, if you use docker to execute.
Example: docker run -p 8890:8890 --name client -e QUERY=... -e F=ttl -e C=gz -e REPO=test dbpedia-client

4. You can load any ?file query. You have the choice either to pass the query directly as a program variable (-e QUERY="..."), or save a query in a file and pass the filepath as variable
5. You can choose different compression formats and additionally you can choose between "same" (no change of compression) or "no" for no compression

IMPORTANT: If you use docker to execute, you can't change the repository yet.

-----------------------------------------
Example(Terminal) (pass the program variable --help to get following information): 

scalac main.scala
scala main -q ./src/query/downloadquery --repo converted_files/ --compression gz -f ttl

For usage see below:
    
  -c, --compression  <arg>   set the compressionformat of the outputfile
  -f, --format  <arg>        set the fileformat of the outputfile
  -q, --query  <arg>         any ?file query; You can pass the query directly or save it in a textfile and pass the filepath
  -r, --repo  <arg>          set the destination directory for converted files
      --help                 Show this message
      

All program variables have default values:
query: the query by default is loaded from the "./src/query/query" file
repo: "./converted_files/"
format: "same"   <-- format conversion isn't working yet
compression: "same"

4. docker stop client

