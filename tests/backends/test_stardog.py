import unittest
import stardog
import os
from pathlib import Path
from tripper.literal import Literal
from rdflib import BNode
from rdflib import URIRef
from pybacktrip.backends.stardog import StardogStrategy

TRIPLESTORE_HOST = "localhost"
TRIPLESTORE_PORT = 5820


class Stardog_TestCase(unittest.TestCase):
    ## Initialization

    @classmethod
    def setUpClass(cls):
        ## Databases currently on stardog so you can ignore them when running tests
        ## Access data initialization for PyStardog

        cls.__endpoint = "http://{}:{}".format(TRIPLESTORE_HOST, TRIPLESTORE_PORT)
        cls.__admin: stardog.Admin = stardog.Admin(endpoint=cls.__endpoint)
        cls.__existing_databases = list(map(lambda x: x.name, cls.__admin.databases()))
        cls.__connection_details = {
            "endpoint": cls.__endpoint,
            "username": "admin",
            "password": "admin",
        }

    def setUp(self):
        ## Creation of a database for the execution of individual tests
        ## Creating the StardogStrategy class

        self.__database_name = "stardog_test"
        self.__database = self.__admin.new_database(self.__database_name)
        self.__connection = stardog.Connection(
            self.__database_name, **self.__connection_details
        )
        self.__triplestore: StardogStrategy = StardogStrategy(
            base_iri="http://example.com/ontology#",
            triplestore_url=self.__endpoint,
            database=self.__database_name,
        )
        self.__existing_namespaces = self.__database.namespaces()  # type: ignore

    @classmethod
    def tearDownClass(cls):
        pass

    def tearDown(self):
        ## Removal of test-created databases if exist
        currently_existing_dbs = list(map(lambda x: x.name, self.__admin.databases()))
        newly_created_dbs = set(currently_existing_dbs) ^ set(self.__existing_databases)

        for db in newly_created_dbs:
            try:
                self.__admin.database(db).drop()
            except Exception as err:
                print("Database {} already deleted...skipping".format(db))

    ## Unit test

    def test_list_databases(self):
        databases = StardogStrategy.list_databases(self.__endpoint)
        self.assertEqual(len(databases), len(self.__existing_databases) + 1)
        self.assertCountEqual(
            databases, self.__existing_databases + [self.__database_name]
        )

    def test_create_database(self):
        new_database_name = "stardog_test_creation"
        creation_response = StardogStrategy.create_database(
            self.__endpoint, new_database_name
        )
        new_databases = list(map(lambda x: x.name, self.__admin.databases()))
        self.assertIsNone(creation_response)
        self.assertEqual(len(new_databases), len(self.__existing_databases) + 2)
        self.assertTrue(new_database_name in new_databases)

    def test_remove_database(self):
        deletion_response = StardogStrategy.remove_database(
            self.__endpoint, self.__database_name
        )
        new_databases = list(map(lambda x: x.name, self.__admin.databases()))
        self.assertIsNone(deletion_response)
        self.assertEqual(len(new_databases), len(self.__existing_databases))
        self.assertFalse(self.__database_name in new_databases)

    def test_parse(self):
        ontology_file_path_ttl = str(
            Path(
                str(Path(__file__).parent.parent.resolve())
                + os.path.sep.join(["", "ontologies", "food.ttl"])
            )
        )
        ontology_file_path_rdf = str(
            Path(
                str(Path(__file__).parent.parent.resolve())
                + os.path.sep.join(["", "ontologies", "food.rdf"])
            )
        )

        print(ontology_file_path_rdf)
        print(ontology_file_path_ttl)

        self.parseTestSkeleton(
            input_format="turtle",
            input_type="source",
            ontology_file_path=ontology_file_path_ttl,
        )
        self.parseTestSkeleton(
            input_format="turtle",
            input_type="location",
            ontology_file_path=ontology_file_path_ttl,
        )
        self.parseTestSkeleton(
            input_format="turtle",
            input_type="data",
            ontology_file_path=ontology_file_path_ttl,
        )

        self.parseTestSkeleton(
            input_format="rdf",
            input_type="source",
            ontology_file_path=ontology_file_path_rdf,
        )
        self.parseTestSkeleton(
            input_format="rdf",
            input_type="location",
            ontology_file_path=ontology_file_path_rdf,
        )
        self.parseTestSkeleton(
            input_format="rdf",
            input_type="data",
            ontology_file_path=ontology_file_path_rdf,
        )

    def test_serialize(self):
        ontology_file_path = str(
            Path(
                str(Path(__file__).parent.parent.resolve())
                + os.path.sep.join(["", "ontologies", "food.ttl"])
            )
        )
        self.__connection.begin()
        self.__connection.add(stardog.content.File(ontology_file_path))
        self.__connection.commit()

        db_content = self.__triplestore.serialize()
        with open(
            str(
                Path(
                    str(Path(__file__).parent.parent.resolve())
                    + os.path.sep.join(["", "ontologies", "expected_ontology.ttl"])
                )
            ),
            "r",
        ) as out_file:
            expected_serialization = out_file.read()

        self.assertEqual(expected_serialization, db_content)

    def test_add_triples(self):
        triple_1 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://www.w3.org/2002/07/owl#Class>",
            )
        ]
        triple_2 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
                "<http://onto-ns.com/ontologies/examples/food#FOOD_d2741ae5_f200_4873_8f72_ac315917c41b>",
            )
        ]
        triple_3 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2004/02/skos/core#prefLabel>",
                '"Carrot"@en',
            )
        ]

        self.__triplestore.add_triples(triple_1 + triple_2 + triple_3)

        query_result = self.__connection.select(
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }", reasoning=False
        )
        triples = self.parseQueryResult(query_result)  # type: ignore
        # converted_triples = self.normalizeTriples(triples)

        self.assertEqual(len(triples), 3)
        self.assertCountEqual(triples, triple_1 + triple_2 + triple_3)

    def test_add_triples_differentformat(self):
        triple_1 = [
            (
                ":FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://www.w3.org/2002/07/owl#Class>",
            )
        ]

        self.__triplestore.add_triples(triple_1)

        query_result = self.__connection.select(
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }", reasoning=False
        )
        triples = self.parseQueryResult(query_result)  # type: ignore
        # converted_triples = self.normalizeTriples(triples)

        self.assertEqual(len(triples), 1)

    def test_triples(self):
        triple_1 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://www.w3.org/2002/07/owl#Class>",
            )
        ]
        triple_2 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
                "<http://onto-ns.com/ontologies/examples/food#FOOD_d2741ae5_f200_4873_8f72_ac315917c41b>",
            )
        ]
        triple_3 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2004/02/skos/core#prefLabel>",
                '"Carrot"@en',
            )
        ]
        to_add = triple_1 + triple_2 + triple_3

        self.__database.remove_namespace("")  # type:ignore
        self.__database.remove_namespace("stardog")  # type:ignore
        self.__database.add_namespace(
            "", "http://onto-ns.com/ontologies/examples/food#"
        )  # type:ignore

        self.__connection.begin()
        for triple in to_add:
            self.__connection.add(
                stardog.content.Raw("{} {} {}".format(*triple), "text/turtle")
            )
        self.__connection.commit()

        triples_set_1 = list(self.__triplestore.triples((None, "<http://www.w3.org/2004/02/skos/core#prefLabel>", None)))  # type: ignore
        converted_triples_set_1 = self.normalizeTriples(triples_set_1)

        self.assertEqual(len(triples_set_1), 1)
        self.assertCountEqual(converted_triples_set_1, triple_3)

        triples_set_2 = list(self.__triplestore.triples(("<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>", None, "<http://www.w3.org/2002/07/owl#Class>")))  # type: ignore
        converted_triples_set_2 = self.normalizeTriples(triples_set_2)
        self.assertEqual(len(triples_set_2), 1)
        self.assertCountEqual(converted_triples_set_2, triple_1)

    def test_remove(self):
        triple_1 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://www.w3.org/2002/07/owl#Class>",
            )
        ]
        triple_2 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
                "<http://onto-ns.com/ontologies/examples/food#FOOD_d2741ae5_f200_4873_8f72_ac315917c41b>",
            )
        ]
        triple_3 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2004/02/skos/core#prefLabel>",
                '"Carrot"@en',
            )
        ]
        to_add = triple_1 + triple_2 + triple_3

        self.__connection.begin()
        for triple in to_add:
            self.__connection.add(
                stardog.content.Raw("{} {} {}".format(*triple), "text/turtle")
            )
        self.__connection.commit()

        self.__triplestore.remove(triple_2[0])
        query_result = self.__connection.select(
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }", reasoning=False
        )
        triples = self.parseQueryResult(query_result)  # type: ignore

        self.assertEqual(len(triples), 2)
        self.assertCountEqual(triples, triple_1 + triple_3)

        self.__triplestore.remove(triple_3[0])
        query_result = self.__connection.select(
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }", reasoning=False
        )
        triples = self.parseQueryResult(query_result)  # type: ignore

        self.assertEqual(len(triples), 1)
        self.assertCountEqual(triples, triple_1)

    def test_query(self):
        triple_1 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://www.w3.org/2002/07/owl#Class>",
            )
        ]
        triple_2 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
                "<http://onto-ns.com/ontologies/examples/food#FOOD_d2741ae5_f200_4873_8f72_ac315917c41b>",
            )
        ]
        triple_3 = [
            (
                "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                "<http://www.w3.org/2004/02/skos/core#prefLabel>",
                '"Carrot"@en',
            )
        ]
        to_add = triple_1 + triple_2 + triple_3

        self.__connection.begin()
        for triple in to_add:
            self.__connection.add(
                stardog.content.Raw("{} {} {}".format(*triple), "text/turtle")
            )
        self.__connection.commit()

        matching_triples_1 = self.__triplestore.query(
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }", resoning=False
        )
        converted_triples_set_1 = self.normalizeTriples(matching_triples_1)
        matching_triples_2 = self.__triplestore.query(
            "SELECT ?s ?o WHERE { ?s rdf:type ?o . }", resoning=False
        )
        converted_triples_set_2 = self.normalizeTriples(matching_triples_2)

        self.assertEqual(len(converted_triples_set_1), 3)
        self.assertCountEqual(converted_triples_set_1, triple_1 + triple_2 + triple_3)
        self.assertEqual(len(converted_triples_set_2), 1)
        self.assertCountEqual(
            converted_triples_set_2, [(triple_1[0][0], triple_1[0][2])]
        )

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
        current_namespaces = self.__database.namespaces()  # type:ignore

        self.assertEqual(len(current_namespaces), len(self.__existing_namespaces) + 1)
        found = False
        for namespace in current_namespaces:
            if (
                namespace["prefix"] == "food"
                and namespace["name"] == "http://onto-ns.com/ontologies/examples/food#"
            ):
                found = True
                break
        self.assertTrue(found)

    def test_bind_deletion(self):
        self.__triplestore.bind("owl", None)  # type:ignore
        current_namespaces = self.__database.namespaces()  # type:ignore

        self.assertEqual(len(current_namespaces), len(self.__existing_namespaces) - 1)
        not_found = True
        for namespace in current_namespaces:
            if namespace["prefix"] == "owl":
                not_found = False
                break
        self.assertTrue(not_found)

    ## Utils functions

    def parseTestSkeleton(
        self, input_format, input_type, ontology_file_path, input_encoding="utf8"
    ):
        if input_type == "source":
            self.__triplestore.parse(
                source=open(ontology_file_path, "r", encoding=input_encoding),
                format=input_format,
            )
        elif input_type == "location":
            self.__triplestore.parse(location=ontology_file_path, format=input_format)
        else:
            with open(ontology_file_path, "r", encoding=input_encoding) as file:
                self.__triplestore.parse(data=file.read(), format=input_format)

        db_content = self.__connection.export(stardog.content_types.TURTLE).decode()  # type: ignore

        with open(
            str(
                Path(
                    str(Path(__file__).parent.parent.resolve())
                    + os.path.sep.join(["", "ontologies", "expected_ontology.ttl"])
                )
            ),
            "r",
        ) as out_file:
            expected_serialization = out_file.read()

        self.assertEqual(expected_serialization, db_content)

    def parseQueryResult(self, query_result: dict):
        query_vars = query_result["head"]["vars"]  # type: ignore
        query_bindings = query_result["results"]["bindings"]  # type: ignore

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
