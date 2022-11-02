# Databus Client Python

## Install
```bash
python3 -m pip install databusclient
```

## CLI Usage
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
### Deploy command
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
Examples of using deploy command
```
databusclient deploy --versionid https://databus.dbpedia.org/user1/group1/artifact1/2022-05-18 --title title1 --abstract abstract1 --description description1 --license http://dalicc.net/licenselibrary/AdaptivePublicLicense10 --apikey MYSTERIOUS 'https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger'  
```

```
databusclient deploy --versionid https://dev.databus.dbpedia.org/denis/group1/artifact1/2022-05-18 --title "Client Testing" --abstract "Testing the client...." --description "Testing the client...." --license http://dalicc.net/licenselibrary/AdaptivePublicLicense10 --apikey MYSTERIOUS 'https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger'  
```

A few more notes for CLI usage:

* The content variants can be left out ONLY IF there is just one distribution
  * For complete inferred: Just use the URL with `https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml`
  * If other parameters are used, you need to leave them empty like `https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml||yml|7a751b6dd5eb8d73d97793c3c564c71ab7b565fa4ba619e4a8fd05a6f80ff653:367116`

## Module Usage

### Step 1: Create lists of distributions for the dataset

```python
from databusclient import create_distribution

# create a list
distributions = []

# minimal requirements
# compression and filetype will be inferred from the path
# this will trigger the download of the file to evaluate the shasum and content length
distributions.append(
    create_distribution(url="https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml", cvs={"type": "swagger"})
)

# full parameters
# will just place parameters correctly, nothing will be downloaded or inferred
distributions.append(
    create_distribution(
        url="https://example.org/some/random/file.csv.bz2", 
        cvs={"type": "example", "realfile": "false"}, 
        file_format="csv", 
        compression="bz2", 
        sha256_length_tuple=("7a751b6dd5eb8d73d97793c3c564c71ab7b565fa4ba619e4a8fd05a6f80ff653", 367116)
    )
)
```

A few notes:

* The dict for content variants can be empty ONLY IF there is just one distribution
* There can be no compression if there is no file format

### Step 2: Create dataset

```python
from databusclient import createDataset

# minimal way
dataset = createDataset(
    version_id="https://dev.databus.dbpedia.org/denis/group1/artifact1/2022-05-18",
    title="Client Testing",
    abstract="Testing the client....",
    description="Testing the client....",
    license_url="http://dalicc.net/licenselibrary/AdaptivePublicLicense10",
    distributions=distributions,
)

# with group metadata
dataset = createDataset(
    version_id="https://dev.databus.dbpedia.org/denis/group1/artifact1/2022-05-18",
    title="Client Testing",
    abstract="Testing the client....",
    description="Testing the client....",
    license_url="http://dalicc.net/licenselibrary/AdaptivePublicLicense10",
    distributions=distributions,
    group_title="Title of group1",
    group_abstract="Abstract of group1",
    group_description="Description of group1"
)
```

NOTE: To be used you need to set all group parameters, or it will be ignored

### Step 3: Deploy to databus

```python
from databusclient import deploy

# to deploy something you just need the dataset from the previous step and an APIO key
# API key can be found (or generated) at https://$$DATABUS_BASE$$/$$USER$$#settings
deploy(dataset, "mysterious api key")
```