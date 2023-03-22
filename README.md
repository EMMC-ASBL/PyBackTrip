# PyBackTrip
*A Pyhton-based backends implementation for the [Tripper](https://github.com/EMMC-ASBL/tripper) tool*

---
## Triplestore supported
PyBackTrip currently supports the following triplestore solutions:
* Stardog
---

## Installation
PyBackTrip relies on the dependencies of the specific triplestores supported. The package can be installed through its **pyproject.toml** file

```python
pip install .
```

Alternatively, in case you are interested in just one of the possible backends, you can install it explicitly by using its name

```python
pip install .[stardog]
```

---
## How to use
The package provides several backends implementations. After installing they are automatically available inside the tripper leveraging the entry-point system.

```python
from tripper import Triplestore

ts = Triplestore(backend="stardog", ...)
```
---

## Backend specific details
Each backend may have its own configuration in the Triplestore class. This section provides usage examples for each of them

### Stardog
```python
from tripper import Triplestore

ts = Triplestore(backend="stardog", base_iri="http://localhost:5820", database="database")
```
* **base_iri**: the Stardog service endpoint
* **database**: the name of the database to use



