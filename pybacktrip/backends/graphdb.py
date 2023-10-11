
import requests
import io
from tripper import Literal as TripperLiteral
from typing import TYPE_CHECKING
from SPARQLWrapper import GET, JSON, POST, RDFXML, SPARQLWrapper
from rdflib import Graph, Namespace, URIRef, Literal, BNode

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Dict, Generator

    from SPARQLWrapper import QueryResult
    from tripper.triplestore import Triple


class GraphDBStrategy():

    ## Class attributes
    __database: str = None  #type: ignore
    __triplestore_url: str = None #type: ignore
    __serialization_format_supported = ["turtle"]
    __parsing_format_supported = ["turtle"]
    __content_types = {
        "turtle": "application/x-turtle",
    }
    __file_extension = {
        "turtle": ".ttl",
    }
   

    def __init__(self, base_iri: str, triplestore_url: str, database: str, **kwargs) -> None:

        headers = {
            'Accept': 'application/json',
        }

        # Check if triplestore_url is a valid url and reachable for graphdb
        try:
            response = requests.get(url = "{}/rest/locations/active".format(triplestore_url), headers = headers)
            if response.status_code == 200:
                print("GraphDB reachable")
            else:
                print(response.status_code)
                print(response.text)
                raise Exception("GraphDB not reachable")
        except Exception as err:
            print("Exception occurred during connection to graphdb: {}".format(err))
            raise err      

        # Check if database exists
        try:
            response = requests.get(url = "{}/rest/repositories/{}".format(triplestore_url, database), headers = headers)
            if response.status_code == 200:
                print("Database {} exists".format(database))
                self.__database = database
                self.__triplestore_url = triplestore_url
                self.__sparql_endpoint = SPARQLWrapper(endpoint="{}/repositories/{}".format(triplestore_url, database), **kwargs)
            else:
                print(response.status_code)
                print(response.text)
                raise Exception("Database {} does not exist".format(database))
            
        except Exception as err:
            print("Exception occurred during connection to database {}: {}".format(database, err))
            raise err    


        print("GraphDBStrategy for database {} initialized".format(self.__database))


    @classmethod
    def list_databases(cls, triplestore_url: str, **kwargs):
        databases = []
        
        headers = {
            'Accept': 'application/json',
        }

        try:
            # response = requests.get(url = "{}/rest/locations/active".format(triplestore_url), headers = headers)
            # if response.status_code == 200:
            #     print("GraphDB reachable")

            response = requests.get('{}/rest/repositories'.format(triplestore_url), headers=headers)
            if response.status_code == 200:
                for database in response.json():
                    databases.append(database['id'])

        except Exception as err:
            print("Exception occurred during connection to graphdb: {}".format(err))        

        return databases
    
    
    @classmethod
    def create_database(cls, triplestore_url: str, database: str, **kwargs):
        headers_get = {
            'Accept': 'application/json',
        }
        headers_post = {
            'Accept': '*/*',
        }


        try:
            # response = requests.get(url = "{}/rest/locations/active".format(triplestore_url), headers = headers_get)
            # if response.status_code == 200:
            #     print("GraphDB reachable")

            response = requests.post('http://127.0.0.1:7200/rest/repositories', headers=headers_post, files={'config': GraphDBStrategy.__create_configuration_ttl(database)})
            if response.status_code == 201 or response.status_code == 400:
                print("Database {} created".format(database))
                return database
            else:
                print(response.status_code)
                print(response.text)
                print("Database {} not created".format(database))

        except Exception as err:
            print("Exception occurred during connection to graphdb: {}".format(err))


    
    @classmethod
    def remove_database(cls, triplestore_url: str, database: str, **kwargs):
        headers = {
            'Accept': '*/*',
        }

        try:
            # response = requests.get(url = "{}/rest/locations/active".format(triplestore_url), headers = headers)
            # if response.status_code == 200:
            #     print("GraphDB reachable")

            response = requests.delete('{}/rest/repositories/{}'.format(triplestore_url, database), headers=headers)
            if response.status_code == 200:
                print("Database {} deleted".format(database))
            else:
                print(response.status_code)
                print(response.text)
                print("Database {} not deleted".format(database))

        except Exception as err:
            print("Exception occurred during connection to graphdb: {}".format(err))



    def parse(self, source=None, location=None, data=None, format="turtle", **kwargs):

        if data is not None:
            raise Exception("GraphDB backend does not support parsing data")

        if format not in self.__parsing_format_supported:
            raise Exception("Format not supported")

        content = None
        headers = {
            'Content-Type': self.__content_types[format],
        }

        if source is not None and isinstance(source, (io.IOBase, io.TextIOBase, io.BufferedIOBase, io.RawIOBase)):
            content = source.read()

        elif (source is not None and isinstance(source, str)) or (location is not None):
            content = open(source if source is not None else location, "r").read() #type: ignore

        else:
            raise Exception("Error during argument checking\nOnly one among source, location must be provided\n")
        

        try:
            response = requests.post('{}/repositories/{}/statements'.format(self.__triplestore_url, self.__database), headers=headers, data=content)
            if response.status_code == 204:
                print("Data added to database {}".format(self.__database))
            else:
                print(response.status_code)
                print(response.text)
                print("Data not added to database {}".format(self.__database))
        except Exception as err:
            print("Exception occurred during connection to graphdb: {}".format(err))


    def triples(self, triple: "Triple") -> "Generator[Triple, None, None]":
        variables = [
            f"?{triple_name}"
            for triple_name, triple_value in zip("spo", triple)
            if triple_value is None
        ]
        if not variables: variables.append("*")
        where_spec = " ".join(
            f"?{triple_name}"
            if triple_value is None
            else triple_value
            if triple_value.startswith("<")
            # else "<{}{}>".format(self.__getBaseNamespace(), triple_value[1:])
            # if triple_value.startswith(":")
            else f"<{triple_value}>"
            for triple_name, triple_value in zip("spo", triple)
        )
        query = "\n".join(
            [
                f"SELECT {' '.join(variables)} WHERE {{",
                f"  {where_spec} .",
                "}",
            ]
        )
        self.__sparql_endpoint.setReturnFormat(JSON)
        self.__sparql_endpoint.setMethod(GET)
        self.__sparql_endpoint.setQuery(query)
        ret = self.__sparql_endpoint.queryAndConvert()
        for binding in ret["results"]["bindings"]:  # type: ignore              
            yield tuple(
                self.__convert_json_entrydict(binding[name]) if name in binding else value  # type: ignore
                for name, value in zip("spo", triple)
            )




    ### Utils methods
    @classmethod
    def __create_configuration_ttl(cls, database_name : str):

        # Define namespaces
        RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        REP = Namespace("http://www.openrdf.org/config/repository#")
        SR = Namespace("http://www.openrdf.org/config/repository/sail#")
        SAIL = Namespace("http://www.openrdf.org/config/sail#")
        OWLIM = Namespace("http://www.ontotext.com/trree/owlim#")

        # Create graph
        g = Graph()

        # Bind namespaces
        g.bind("rdf", RDF)
        g.bind("rdfs", RDFS)
        g.bind("rep", REP)
        g.bind("sr", SR)
        g.bind("sail", SAIL)
        g.bind("owlim", OWLIM)

        # Add triples
        g.add((URIRef(""), RDF.type, REP.Repository))
        g.add((URIRef(""), REP.repositoryID, Literal(database_name)))
        g.add((URIRef(""), RDFS.label, Literal("GRAPHDB TRIPPER REPOSITORY")))
        g.add((URIRef(""), REP.repositoryImpl, BNode("#impl")))
        g.add((BNode("#impl"), REP.repositoryType, Literal("graphdb:SailRepository")))
        g.add((BNode("#impl"), SR.sailImpl, BNode("#sail")))
        g.add((BNode("#sail"), SAIL.sailType, Literal("graphdb:Sail")))
        g.add((BNode("#sail"), URIRef("http://www.ontotext.com/trree/owlim#entity-index-size"), Literal("100000000"))) #type: ignore

        # Serialize graph to Turtle format
        # print(g.serialize(format="turtle"))

        return g.serialize(format="turtle")
    

    def __convert_json_entrydict(self, entrydict: "Dict[str, str]") -> str:  # type: ignore
        if entrydict["type"] == "uri":
            # if entrydict["value"].startswith(":"):
            #     return "<{}{}>".format(self.__getBaseNamespace(), entrydict["value"][1:])
            # else:
            return URIRef(entrydict["value"]).n3()

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

        raise ValueError(f"unexpected type in entrydict: {entrydict}")
    

## TEST ##

print(GraphDBStrategy.list_databases("http://localhost:7200"))
GraphDBStrategy.remove_database("http://localhost:7200", "test")
print(GraphDBStrategy.list_databases("http://localhost:7200"))
GraphDBStrategy.create_database("http://localhost:7200", "test")
print(GraphDBStrategy.list_databases("http://localhost:7200"))

graphdb = GraphDBStrategy(triplestore_url = "http://localhost:7200", database = "test", base_iri = "")
graphdb.parse(source = r"C:\Users\alexc\Desktop\Universita\Ricerca\OntoTrans\oip-workshop\ontology\solvents.ttl", format = "turtle")
graphdb.parse(source = r"C:\Users\alexc\Desktop\Universita\Ricerca\OntoTrans\oip-workshop\ontology\solvents-individuals.ttl", format = "turtle")
graphdb.parse(source = r"C:\Users\alexc\Desktop\Universita\Ricerca\OntoTrans\oip-workshop\entities\isobaric_liquids_nist.ttl", format = "turtle")
graphdb.parse(source = r"C:\Users\alexc\Desktop\Universita\Ricerca\OntoTrans\oip-workshop\entities\isobaric_liquids_nist.ttl", format = "turtle")
graphdb.parse(source = r"C:\Users\alexc\Desktop\Universita\Ricerca\OntoTrans\oip-workshop\entities\md_aa.ttl", format = "turtle")
graphdb.parse(source = r"C:\Users\alexc\Desktop\Universita\Ricerca\OntoTrans\oip-workshop\entities\md_cg.ttl", format = "turtle")

matching_triples = graphdb.triples(triple = ("<http://ontotrans.eu/meta/1.0/metadata#oip-benzene>", None, None)) #type: ignore
print(list(matching_triples))  



