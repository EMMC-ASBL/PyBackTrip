import os
import unittest

import requests
from pybacktrip.backends.fuseki import FusekiStrategy
from rdflib import BNode, URIRef
from tripper.literal import Literal

TRIPLESTORE_HOST = "localhost"
TRIPLESTORE_PORT = 3030
DATABASE = "openmodel"
GRAPH = "graph://main"


class Fuseki_TestCase(unittest.TestCase):
    ## Initialization

    def setUp(self):
        ## Creation of a database for the execution of individual tests
        ## Creating the StardogStrategy class

        self.triplestore: FusekiStrategy = FusekiStrategy(
            base_iri="http://example.com/ontology#",
            triplestore_url=f"http://{TRIPLESTORE_HOST}:{TRIPLESTORE_PORT}",
            database="openmodel",
        )

        self.__existing_namespaces = self.triplestore.namespaces().copy()

    def tearDown(self):
        FusekiStrategy.remove_database(
            triplestore_url=f"http://{TRIPLESTORE_HOST}:{TRIPLESTORE_PORT}",
            database=DATABASE,
        )

    ## Unit test

    ## DEFAULT METHODS

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

        self.triplestore.add_triples(to_add)

        triples_set_1 = list(
            self.triplestore.triples(
                (None, "<http://www.w3.org/2004/02/skos/core#prefLabel>", None)
            )
        )
        converted_triples_set_1 = self._normalizeTriples(triples_set_1)

        self.assertEqual(len(triples_set_1), 1)
        self.assertCountEqual(converted_triples_set_1, triple_3)

        triples_set_2 = list(
            self.triplestore.triples(
                (
                    "<http://onto-ns.com/ontologies/examples/food#FOOD_e9cb271c_3be0_44e4_960f_6f6676445dbb>",
                    None,
                    "<http://www.w3.org/2002/07/owl#Class>",
                )
            )
        )
        converted_triples_set_2 = self._normalizeTriples(triples_set_2)
        self.assertEqual(len(triples_set_2), 1)
        self.assertCountEqual(converted_triples_set_2, triple_1)

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

        self.triplestore.add_triples(triple_1 + triple_2 + triple_3)

        query_result = self._selectAll()
        triples = self._parseQueryResult(query_result)

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

        self.triplestore.add_triples(triple_1)

        query_result = self._selectAll()
        triples = self._parseQueryResult(query_result)
        # converted_triples = self.normalizeTriples(triples)

        self.assertEqual(len(triples), 1)

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

        self.triplestore.add_triples(to_add)

        self.triplestore.remove(triple_2[0])
        query_result = self._selectAll()
        triples = self._parseQueryResult(query_result)

        self.assertEqual(len(triples), 2)
        self.assertCountEqual(triples, triple_1 + triple_3)

        self.triplestore.remove(triple_3[0])
        query_result = self._selectAll()
        triples = self._parseQueryResult(query_result)

        self.assertEqual(len(triples), 1)
        self.assertCountEqual(triples, triple_1)

    ## ADDITIONAL METHODS

    def test_parse(self):
        ontology_file_path_ttl = os.path.abspath("tests/ontologies/food.ttl")
        ontology_file_path_rdf = os.path.abspath("tests/ontologies/food.rdf")

        self._parseTestSkeleton(
            input_format="turtle",
            input_type="source",
            ontology_file_path=ontology_file_path_ttl,
        )
        self._parseTestSkeleton(
            input_format="turtle",
            input_type="location",
            ontology_file_path=ontology_file_path_ttl,
        )
        self._parseTestSkeleton(
            input_format="turtle",
            input_type="data",
            ontology_file_path=ontology_file_path_ttl,
        )

        self._parseTestSkeleton(
            input_format="rdf",
            input_type="source",
            ontology_file_path=ontology_file_path_rdf,
        )
        self._parseTestSkeleton(
            input_format="rdf",
            input_type="location",
            ontology_file_path=ontology_file_path_rdf,
        )
        self._parseTestSkeleton(
            input_format="rdf",
            input_type="data",
            ontology_file_path=ontology_file_path_rdf,
        )

    def test_serialize(self):
        ontology_file_path = os.path.abspath("tests/ontologies/food.ttl")

        self.triplestore.parse(ontology_file_path)
        db_content = self.triplestore.serialize()

        with open(
            os.path.abspath("tests/ontologies/fuseki_expected_ontology.ttl"),
            "r",
        ) as out_file:
            expected_serialization = out_file.read()

        self.assertEqual(expected_serialization, db_content)

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

        self.triplestore.add_triples(to_add)

        matching_triples_1 = self.triplestore.query(
            f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o . }}"
        )
        converted_triples_set_1 = self._normalizeTriples(matching_triples_1)
        matching_triples_2 = self.triplestore.query(
            f"SELECT ?s ?o WHERE {{ ?s rdf:type ?o . }}"
        )
        converted_triples_set_2 = self._normalizeTriples(matching_triples_2)

        self.assertEqual(len(converted_triples_set_1), 3)
        self.assertCountEqual(converted_triples_set_1, triple_1 + triple_2 + triple_3)
        self.assertEqual(len(converted_triples_set_2), 1)
        self.assertCountEqual(
            converted_triples_set_2, [(triple_1[0][0], triple_1[0][2])]
        )

    def test_bind(self):
        self.triplestore.bind("food", "http://onto-ns.com/ontologies/examples/food#")
        current_namespaces = self.triplestore.namespaces()

        self.assertEqual(len(current_namespaces), len(self.__existing_namespaces) + 1)
        found = False
        for k, v in current_namespaces.items():
            if k == "food" and v == "http://onto-ns.com/ontologies/examples/food#":
                found = True
                break
        self.assertTrue(found)

    def test_bind_deletion(self):
        self.triplestore.bind("owl", None)
        current_namespaces = self.triplestore.namespaces()

        self.assertEqual(len(current_namespaces), len(self.__existing_namespaces) - 1)
        not_found = True
        for k in current_namespaces:
            if k == "owl":
                not_found = False
                break
        self.assertTrue(not_found)

    def test_namespaces(self):
        namespaces = self.triplestore.namespaces()

        self.assertEqual(len(namespaces.keys()), len(self.__existing_namespaces))
        for k, v in self.__existing_namespaces.items():
            prefix = k
            uri = v

            self.assertTrue(prefix in namespaces)
            self.assertEqual(uri, namespaces[prefix])

    ## Utils functions

    def _selectAll(self):
        return requests.get(
            f"http://{TRIPLESTORE_HOST}:{TRIPLESTORE_PORT}/{DATABASE}",
            params={"query": f"SELECT ?s ?p ?o FROM <{GRAPH}> WHERE {{ ?s ?p ?o . }}"},
        ).json()

    def _parseTestSkeleton(
        self, input_format, input_type, ontology_file_path, input_encoding="utf8"
    ):
        if input_type == "source":
            self.triplestore.parse(
                source=open(ontology_file_path, "rb"),
                format=input_format,
            )
        elif input_type == "location":
            self.triplestore.parse(location=ontology_file_path, format=input_format)
        else:
            with open(ontology_file_path, "rb") as file:
                self.triplestore.parse(data=file.read(), format=input_format)

        db_content = self.triplestore.serialize()

        with open(
            os.path.abspath(
                f"tests/ontologies/fuseki_expected_ontology.{'rdf' if input_format == 'rdf' else 'ttl'}"
            ),
            "r",
        ) as out_file:
            expected_serialization = out_file.read()

        self.assertEqual(expected_serialization, db_content)

    def _parseQueryResult(self, query_result: dict):
        query_vars = query_result["head"]["vars"]
        query_bindings = query_result["results"]["bindings"]

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
            triples_res.append(self._normalizeTriple(current_triple))

        return triples_res

    def _normalizeTriple(self, triple):
        converted_triple = ()
        for value in triple:
            converted_value = self._asuristr(value)
            converted_triple = converted_triple + (converted_value,)

        return converted_triple

    def _normalizeTriples(self, triples):
        converted_triples = []
        for triple in triples:
            converted_triples.append(self._normalizeTriple(triple))

        return converted_triples

    def _asuristr(self, value):
        if value is None:
            return None
        if isinstance(value, Literal):
            return value.n3()
        if value.startswith("_:"):
            return BNode(value).n3()
        if value.startswith("<"):
            value = value[1:-1]
        return URIRef(value).n3()
