import requests
from io import BufferedReader
from typing import IO, TYPE_CHECKING, Union, Literal as L
from tripper import Literal

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Generator
    from tripper.triplestore import Triple


class FusekiStrategy:
    __GRAPH = "graph://main"
    __CONTENT_TYPES = {"turtle": "text/turtle", "rdf": "application/rdf+xml"}
    __DEFAULT_NAMESPACES = {
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "schema": "http://schema.org/",
    }

    # DEFAULT METHODS

    def __init__(
        self,
        base_iri: str,
        triplestore_url: str,
        database: str,
        **kwargs,
    ) -> None:
        """Initialise triplestore.

        Args:
            baseIri (str): Optional base IRI to initiate the triplestore from.
            triplestoreUrl (str): URL of the Triplestore.
            database (str): Database of the Triplestore to be used.
            kwargs (object): Additional keyword arguments passed to the backend.
        """

        self.__namespaces = {}
        self.prefer_sparql = True  # prefer tripper.query() over tripper.triples()

        self.__namespaces.update(self.__DEFAULT_NAMESPACES)
        self.__namespaces[""] = base_iri
        self.sparql_endpoint = f"{triplestore_url}/{database}"
        self.graph = self.__GRAPH

    def triples(self, triple: "Triple") -> "Generator":
        """Execute query on triples

        Args:
            triple (Triple): A `(s, p, o)` tuple where `s`, `p` and `o` should either be None (matching anything) or an exact IRI to match.

        Yields:
            Generator: Matching triples
        """

        variables = [
            f"?{tripleName}"
            for tripleName, tripleValue in zip("spo", triple)
            if tripleValue is None
        ]
        if not variables:
            variables.append("*")
        whereSpec = " ".join(
            (
                f"?{tripleName}"
                if tripleValue is None
                else (
                    tripleValue
                    if tripleValue.startswith("<")
                    else (
                        "<{}{}>".format(self.__namespaces[""], tripleValue[1:])
                        if tripleValue.startswith(":")
                        else f"<{tripleValue}>"
                    )
                )
            )
            for tripleName, tripleValue in zip("spo", triple)
        )
        cmd = f"""
            SELECT {" ".join(variables)}
            FROM <{self.__GRAPH}>
            WHERE {{{whereSpec}}}
        """

        res = self._request("GET", cmd)

        for binding in res["results"]["bindings"]:
            yield tuple(
                (
                    self.__convert_json_entrydict(binding[name])
                    if name in binding
                    else value
                )
                for name, value in zip("spo", triple)
            )

    def add_triples(self, triples: "Sequence[Triple]") -> dict:
        """Add a sequence of triples.

        Args:
            triples (Sequence[Triple]): A sequence of `(s, p, o)` tuples to add to the triplestore.

        Returns:
            dict: The result of the operation
        """

        spec = " ".join(
            " ".join(
                (
                    value.n3()
                    if isinstance(value, Literal) and hasattr(value, "n3")
                    else (
                        value
                        if value.startswith("<") or value.startswith('"')
                        else (
                            "<{}{}>".format(self.__namespaces[""], value[1:])
                            if "" in self.__namespaces and value.startswith(":")
                            else str(value) if value.startswith(":") else f"<{value}>"
                        )
                    )
                )
                for value in triple
            )
            + " ."
            for triple in triples
        )

        cmd = f"INSERT DATA {{ GRAPH <{self.__GRAPH}> {{ {spec} }} }}"
        headers = {"Content-Type": "application/sparql-update"}
        return self._request("POST", cmd, headers=headers, plainData=True, graph=True)

    def remove(self, triple: "Triple") -> object:
        """Remove all matching triples from the backend.

        Args:
            triple (Triple): A `(s, p, o)` tuple where `s`, `p` and `o` should either be None (matching anything) or an exact IRI to match.

        Returns:
            dict: The result of the operation
        """

        spec = " ".join(
            (
                f"?{name}"
                if value is None
                else (
                    value.n3()
                    if isinstance(value, Literal)
                    else (
                        value
                        if value.startswith("<") or value.startswith('"')
                        else (
                            "<{}{}>".format(self.__namespaces[""], value[1:])
                            if value.startswith(":")
                            else f"<{value}>"
                        )
                    )
                )
            )
            for name, value in zip("spo", triple)
        )
        cmd = f"DELETE WHERE {{ GRAPH <{self.__GRAPH}> {{ { spec } }} }}"

        return self._request("POST", cmd)

    # ADDITIONAL METHODS

    def parse(
        self,
        source: Union[str, IO] = "",
        location: str = "",
        data: str = "",
        format: str = "turtle",
        **kwargs,
    ):
        """Parse source and add the resulting triples to triplestore.

        The source is specified using one of `source`, `location` or `data`.

        Arguments:
            source: File-like object or file name.
            location: String with a URL linked to source.
            data: String containing the data to be parsed.
            format: Needed if format can not be inferred from source.
            kwargs: Additional backend-specific parameters controlling the parsing.
        """

        content = None

        if format not in self.__CONTENT_TYPES:
            raise Exception("Format not supported")

        if source:
            if isinstance(source, str):
                content = open(source, "rb")
            else:
                content = source.read()
        elif location:
            resp = requests.get(location)
            resp.raise_for_status()
            content = resp.content
        elif data:
            content = data
        else:
            raise Exception(
                "Error during argument checking\nOnly one among source, location and data must be provided\n"
            )

        headers = {"Content-type": f"{self.__CONTENT_TYPES[format]}"}
        self._request("POST", cmd=content, headers=headers, plainData=True, graph=True)

    def serialize(
        self, destination: Union[str, IO] = "", format: str = "turtle", **kwargs
    ) -> str:
        """Serialise to destination.

        Arguments:
            destination: File name or object to write to. If not defined, the serialisation is returned.
            format: Format to serialise as. Supported formats, depends on the backend.
            kwargs: Additional backend-specific parameters controlling the serialisation.

        Returns:
            Serialised string if `destination` is not defined.
        """

        content = self._request("GET", prefix=False, graph=True, json=False)["response"]

        if not destination:
            return content
        elif isinstance(destination, str):
            with open(destination, "w") as f:
                f.write(content)
        else:
            destination.write(content)

        return ""

    def query(self, query_object: str, **kwargs) -> list:
        """SPARQL query.

        Arguments:
            query_object: String with the SPARQL query.
            kwargs: Additional backend-specific keyword arguments.

        Returns:
            List of tuples of IRIs for each matching row.
        """

        iw = query_object.index("WHERE")
        queryStr = (
            f"{query_object[:iw]}FROM <{self.__GRAPH}> {query_object[iw:]}".strip()
        )

        res = self._request("GET", queryStr)

        queryVars = res["head"]["vars"]
        queryBindings = res["results"]["bindings"]

        triplesRes = []
        for binding in queryBindings:
            currentTriple = ()
            for var in queryVars:
                currentTriple = currentTriple + (
                    self.__convert_json_entrydict(binding[var]),
                )
            triplesRes.append(currentTriple)

        return triplesRes

    def bind(self, prefix: str, namespace: Union[str, None]):
        """Bind, update, or remove a SPARQL namespace

        Args:
            prefix (str): Prefix identifying the namespace
            namespace (str): URI of the namespace
        """

        if namespace:
            self.__namespaces[prefix] = namespace
        else:
            if prefix in self.__namespaces:
                del self.__namespaces[prefix]

    def namespaces(self) -> dict:
        """Get the SPARQL namespaces

        Returns:
            dict: The SPARQL namespaces as dict
        """

        return self.__namespaces

    @classmethod
    def create_database(cls, database: str, **kwargs):
        """Create a new database in backend.

        Args:
            database (str): Name of the new database.
            kwargs: Keyword arguments passed to the backend create_database() method.
        """

        pass

    @classmethod
    def remove_database(cls, triplestore_url: str, database: str, **kwargs):
        """Remove a database in backend.

        Args:
            triplestore_url (str): Endpoint of the triplestore.
            database (str): Name of the database to be removed.
            kwargs: Keyword arguments passed to the backend remove_database() method.
        """

        requests.delete(f"{triplestore_url}/{database}?graph={cls.__GRAPH}")

    @classmethod
    def list_databases(cls, **kwargs) -> str:
        """For backends that supports multiple databases, list of all
        databases.

        Args:
            kwargs: Keyword arguments passed to the backend list_database() method.
        """

        return cls.__GRAPH

    # PROTECTED METHODS

    def _request(
        self,
        method: L["GET", "POST"],
        cmd: Union[str, BufferedReader] = "",
        prefix: bool = True,
        headers: dict = {},
        plainData: bool = False,
        graph: bool = False,
        json: bool = True,
    ) -> dict:
        """Generic REST method caller for the Triplestore

        Args:
            method (Literal["GET", "POST"]): Method of the request.
            cmd (Union[str, BufferedReader], optional): Command to be executed. Defaults to "".
            prefix (bool, optional): If the prefixes need to be added to the query. Defaults to True.
            headers (dict, optional): Custom headers. Defaults to {}.
            plainData (bool, optional): If data needs a format or is plain. Defaults to False.
            graph (bool, optional): If the endpoint needs to specify the graph. Defaults to False.
            json (bool, optional): If the result is a JSON or a dict containing the result as string. Defaults to True.

        Returns:
            dict: Dict containing the result as JSON or text
        """

        if method not in ["GET", "POST"]:
            print("Method unknown")
            return {}

        ep = (
            self.sparql_endpoint
            if not graph
            else f"{self.sparql_endpoint}?graph={self.graph}"
        )

        if prefix and isinstance(cmd, str):
            cmd = (
                " ".join(
                    f"PREFIX {k}: <{v}>" for k, v in self.namespaces().items() if v
                )
                + " "
                + cmd
            )

        try:
            r: requests.Response = requests.request(
                method=method,
                url=ep,
                headers=headers,
                params=({"query": cmd} if method == "GET" and cmd else None),
                data=(
                    cmd
                    if method == "POST" and plainData
                    else {"update": cmd} if method == "POST" and not plainData else None
                ),
            )
            r.raise_for_status()
            if r.status_code == 200:
                return r.json() if json else {"response": r.text}
            return {}
        except requests.RequestException as e:
            print(e)
            return {}

    # PRIVATE METHODS

    def __convert_json_entrydict(self, entrydict: dict) -> str:
        """Convert JSON entry dict in string format

        Args:
            entrydict (dict): Entry dict to be converted

        Raises:
            ValueError: Unexpected type in entrydict

        Returns:
            str: The entry dict correctly formatted as a string
        """

        if entrydict["type"] == "uri":
            if entrydict["value"].startswith(":"):
                return "<{}{}>".format(self.__namespaces[""], entrydict["value"][1:])
            else:
                return entrydict["value"]

        if entrydict["type"] == "literal":
            return Literal(
                entrydict["value"],
                lang=entrydict.get("xml:lang"),
                datatype=entrydict.get("datatype"),
            )

        if entrydict["type"] == "bnode":
            return (
                entrydict["value"]
                if entrydict["value"].startswith("_:")
                else f"_:{entrydict['value']}"
            )

        raise ValueError(f"Unexpected type in entrydict: {entrydict}")
