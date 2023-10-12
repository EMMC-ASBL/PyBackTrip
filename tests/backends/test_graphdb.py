import csv
from io import StringIO
import unittest
import requests
import os
from pathlib import Path
from tripper.literal import Literal as TripperLiteral
from pybacktrip.backends.graphdb import GraphdbStrategy
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from SPARQLWrapper import GET, JSON, POST, RDFXML, SPARQLWrapper


TRIPLESTORE_HOST = "localhost"
TRIPLESTORE_PORT = 7200

class GraphDB_TestCase(unittest.TestCase):

    ## Initialization

    @classmethod
    def setUpClass(cls):
        json_header = {
            'Accept': 'application/json',
        }

        cls.__endpoint = "http://{}:{}".format(TRIPLESTORE_HOST, TRIPLESTORE_PORT)
        cls.__existing_databases = []
        response = requests.get('{}/rest/repositories'.format(cls.__endpoint), headers=json_header)
        if response.status_code == 200:
            for database in response.json():
                 cls.__existing_databases.append(database['id'])

    def setUp(self):

        headers_post = {
            'Accept': '*/*',
        }

        self.__database_name = "graphdb_test"
        response = requests.post('http://127.0.0.1:7200/rest/repositories', headers=headers_post, files={'config': self.__create_configuration_ttl(self.__database_name)})
        response = requests.get('{}/repositories/{}/namespaces'.format(self.__endpoint, self.__database_name))
        self.__existing_namespaces = []
        if response.status_code == 200:
            string_csv = StringIO(response.text)
            reader = csv.reader(string_csv, delimiter=',')
            for row in reader:
                self.__existing_namespaces.append({"prefix": row[0], "name": row[1]})

        self.__triplestore: GraphdbStrategy = GraphdbStrategy(base_iri="http://example.com/ontology#", triplestore_url = self.__endpoint, database=self.__database_name)
        self.__sparql_endpoint_query = SPARQLWrapper(endpoint="{}/repositories/{}".format(self.__endpoint, self.__database_name))

    @classmethod
    def tearDownClass(cls):
        pass

    def tearDown(self):
        ## Removal of test-created databases if exist
        json_header = {
            'Accept': 'application/json',
        }
        delete_headers = {
            'Accept': '*/*',
        }

        currently_existing_dbs = []
        response = requests.get('{}/rest/repositories'.format(self.__endpoint), headers=json_header)
        if response.status_code == 200:
            for database in response.json():
                currently_existing_dbs.append(database['id'])
        newly_created_dbs = set(currently_existing_dbs) ^ set(self.__existing_databases)

        for db in newly_created_dbs:
            try:
                response = requests.delete('{}/rest/repositories/{}'.format(self.__endpoint, db), headers=delete_headers)
            except Exception as err:
                print("Database {} already deleted...skipping".format(db))


    ## Unit test

    def test_list_databases(self):
        databases = GraphdbStrategy.list_databases(self.__endpoint)
        self.assertEqual(len(databases), len(self.__existing_databases) + 1)
        self.assertCountEqual(databases, self.__existing_databases + [self.__database_name])


    def test_create_database(self):
        new_database_name = "graphdb_test_creation"
        creation_response = GraphdbStrategy.create_database(self.__endpoint, new_database_name)
        new_databases = []
        response = requests.get('{}/rest/repositories'.format(self.__endpoint), headers={'Accept': 'application/json'})
        if response.status_code == 200:
            for database in response.json():
                new_databases.append(database['id'])
        self.assertIsNone(creation_response)
        self.assertEqual(len(new_databases), len(self.__existing_databases) + 2)
        self.assertTrue(new_database_name in new_databases)


    def test_remove_database(self):
        deletion_response = GraphdbStrategy.remove_database(self.__endpoint, self.__database_name)
        new_databases = []
        response = requests.get('{}/rest/repositories'.format(self.__endpoint), headers={'Accept': 'application/json'})
        if response.status_code == 200:
            for database in response.json():
                new_databases.append(database['id'])
        self.assertIsNone(deletion_response)
        self.assertEqual(len(new_databases), len(self.__existing_databases))
        self.assertFalse(self.__database_name in new_databases)


    def test_parse(self):
        ontology_file_path_ttl = str(Path(str(Path(__file__).parent.parent.resolve()) + os.path.sep.join(["","ontologies","food.ttl"])))
        ontology_file_path_rdf = str(Path(str(Path(__file__).parent.parent.resolve()) + os.path.sep.join(["","ontologies","food.rdf"])))

        print(ontology_file_path_rdf)
        print(ontology_file_path_ttl)
        
        self.parseTestSkeleton(input_format="turtle", input_type="source", ontology_file_path=ontology_file_path_ttl)
        self.parseTestSkeleton(input_format="turtle", input_type="location", ontology_file_path=ontology_file_path_ttl)


    def test_add_triples(self):
        triple_1 = [("<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<http://www.w3.org/2002/07/owl#Class>")]
        triple_2 = [("<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>", "<http://www.w3.org/2000/01/rdf-schema#subClassOf>","<http://onto-ns.com/ontologies/examples/food#FOOD_d2741ae5_f200_4873_8f72_ac315917c41b>")]
        triple_3 = [("<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>", "<http://www.w3.org/2004/02/skos/core#prefLabel>", "\"Carrot\"@en")]

        self.__triplestore.add_triples(triple_1 + triple_2 + triple_3)

        self.__sparql_endpoint_query.setReturnFormat(JSON)
        self.__sparql_endpoint_query.setMethod(GET)
        self.__sparql_endpoint_query.setQuery("SELECT * WHERE { <http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb> ?P ?O }")
        query_result = self.__sparql_endpoint_query.queryAndConvert()
        query_vars = query_result["head"]["vars"]    # type: ignore
        query_bindings = query_result["results"]["bindings"]     # type: ignore
        triples = self.parseQueryResult(query_result) # type: ignore
        # converted_triples = self.normalizeTriples(triples)

        self.assertEqual(len(triples), 3)
        self.assertCountEqual(triples, [tuple(triple_1[0][1:]), tuple(triple_2[0][1:]), tuple(triple_3[0][1:])])


    def test_namespaces(self):
        namespaces = self.__triplestore.namespaces()

        self.assertEqual(len(namespaces.keys()), len(self.__existing_namespaces))
        for namespace in self.__existing_namespaces:
            prefix = namespace["prefix"]
            uri = namespace["name"]

            self.assertTrue(prefix in namespaces)
            self.assertEqual(uri, namespaces[prefix])


    def test_bind(self):
        self.__triplestore.bind("food", "http://onto-ns.com/ontologies/examples/food#")
        current_namespaces = []
        response = requests.get('{}/repositories/{}/namespaces'.format(self.__endpoint, self.__database_name))
        if response.status_code == 200:
            string_csv = StringIO(response.text)
            reader = csv.reader(string_csv, delimiter=',')
            for row in reader:
                current_namespaces.append({"prefix": row[0], "name": row[1]})

        self.assertEqual(len(current_namespaces), len(self.__existing_namespaces) + 1)
        found = False
        for namespace in current_namespaces:
            if namespace["prefix"] == "food" and namespace["name"] == "http://onto-ns.com/ontologies/examples/food#":
                found = True
                break
        self.assertTrue(found)


    def test_bind_deletion(self):
        self.__triplestore.bind("owl", None) # type:ignore
        current_namespaces = []
        response = requests.get('{}/repositories/{}/namespaces'.format(self.__endpoint, self.__database_name))
        if response.status_code == 200:
            string_csv = StringIO(response.text)
            reader = csv.reader(string_csv, delimiter=',')
            for row in reader:
                current_namespaces.append({"prefix": row[0], "name": row[1]})

        self.assertEqual(len(current_namespaces), len(self.__existing_namespaces) - 1)
        not_found = True
        for namespace in current_namespaces:
            if namespace["prefix"] == "owl":
                not_found = False
                break
        self.assertTrue(not_found)


    ## Utils functions

    def parseTestSkeleton(self, input_format, input_type, ontology_file_path, input_encoding="utf8"):
        if input_type == "source":
            self.__triplestore.parse(source = open(ontology_file_path, "r", encoding=input_encoding), format=input_format)
        elif input_type == "location":
            self.__triplestore.parse(location = ontology_file_path, format=input_format)
        else:
            with open(ontology_file_path, "r", encoding=input_encoding) as file:
                self.__triplestore.parse(data = file.read(), format=input_format)

        self.__sparql_endpoint_query.setReturnFormat(JSON)
        self.__sparql_endpoint_query.setMethod(GET)
        self.__sparql_endpoint_query.setQuery("SELECT * WHERE { <http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb> ?P ?O }")
        query_result = self.__sparql_endpoint_query.queryAndConvert()
        query_vars = query_result["head"]["vars"]    # type: ignore
        query_bindings = query_result["results"]["bindings"]     # type: ignore
        triples = self.parseQueryResult(query_result) # type: ignore

        self.assertEqual(len(triples), 4)


    def parseQueryResult(self, query_result: dict):
        query_vars = query_result["head"]["vars"]    # type: ignore
        query_bindings = query_result["results"]["bindings"]     # type: ignore

        triples_res = []
        for binding in query_bindings:
            current_triple = ()
            for var in query_vars:
                new_value = ""
                entry = binding[var]
                if entry["type"] == "uri":
                    new_value = entry["value"]
                elif entry["type"] == "literal":
                    new_value = Literal(
                        entry["value"],
                        lang=entry.get("xml:lang"),
                        datatype=entry.get("datatype"),
                    )
                elif entry["type"] == "bnode":
                    return (
                        entry["value"]
                        if entry["value"].startswith("_:")
                        else f"_:{entry['value']}"
                    )
                current_triple = current_triple + (new_value,)
            triples_res.append(self.normalizeTriple(current_triple))

        return triples_res

    def normalizeTriple(self, triple):
        converted_triple = ()
        for value in triple:
            converted_value = self.asuristr(value)
            converted_triple = converted_triple + (converted_value,)

        return converted_triple

    def normalizeTriples(self, triples):
        converted_triples = []
        for triple in triples:
            converted_triples.append(self.normalizeTriple(triple))

        return converted_triples


    def asuristr(self, value):
        if value is None:
            return None
        if isinstance(value, Literal):
            return value.n3()
        if value.startswith("_:"):
            return BNode(value).n3()
        if value.startswith("<"):
            value = value[1:-1]
        return URIRef(value).n3()
    

    def __create_configuration_ttl(self, database_name : str):

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



