# Install

use `make` or if not installed use

```
python3 ./setup.py install --user
```

# Run

```
databusclient --help
```

---

# DEPRECATED (below)

## Optional Parameters

Set base of databus:
```
python3 -m databusclient --base http://localhost:3000/
```

Verbose output:
```
python3 -m databusclient --verbose
```

## Examples
```
python3 -m databusclient deploy group --user denis --group test --title "Some Title" --comment "Some comment" --documentation "Some docstring" --file group.jsonld
```