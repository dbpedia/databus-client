# Databus Client Python

## Install
```bash
pip3 install databusclient
```

# CLI Usage
```bash
databusclient --help
```

```man
Usage: databusclient [OPTIONS] COMMAND [ARGS]...

Options:
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.
  --help                          Show this message and exit.

Commands:
  deploy
  downoad
```
## Deploy command
```
databusclient deploy --help
```
```


Usage: databusclient deploy [OPTIONS] DISTRIBUTIONS...

Arguments:
  DISTRIBUTIONS...  distributions in the form of List[URL|CV|fileext|compression|sha256sum:contentlength] where URL is the
                    download URL and CV the key=value pairs (_ separted)
                    content variants of a distribution, fileExt and Compression can be set, if not they are inferred from the path  [required]

Options:
  --versionid TEXT    target databus version/dataset identifier of the form <h
                      ttps://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VE
                      RSION>  [required]
  --title TEXT        dataset title  [required]
  --abstract TEXT     dataset abstract max 200 chars  [required]
  --description TEXT  dataset description  [required]
  --license TEXT      license (see dalicc.net)  [required]
  --apikey TEXT       apikey  [required]
  --help              Show this message and exit.
```
Example of using deploy command
```
databusclient deploy --versionid https://databus.dbpedia.org/user1/group1/artifact1/2022-05-18 --title title1 --abstract abstract1 --description description1 --license http://dalicc.net/licenselibrary/AdaptivePublicLicense10 --apikey MYSTERIOUS 'https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger'  
```

```
databusclient deploy --versionid https://dev.databus.dbpedia.org/denis/group1/artifact1/2022-05-18 --title "Client Testing" --abstract "Testing the client...." --description "Testing the client...." --license http://dalicc.net/licenselibrary/AdaptivePublicLicense10 --apikey MYSTERIOUS 'https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger'  
```

# API Usage
`TODO`
