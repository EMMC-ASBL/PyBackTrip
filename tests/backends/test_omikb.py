from tripper import Triplestore, Literal
import json
# Initialise the Triplestore

# Fixme remove triple_url and base_iri and add service
#TODO: chekc how it's done in discomat in the fuseki engine

ts = Triplestore(
    backend="omikb",
    base_iri="http://example.com/omikb#",
    triplestore_url="https://openmodel.app/kb",
    database="dataset"
)
print("OMIKB Triplestore initialized successfully.")


# Test 1: SPARQL query
sparql_query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10"
#
# print(f"Executing query: {sparql_query}")
#
# Execute the query
result = ts.query(sparql_query)
print("Query Results:")
for binding in result["results"]["bindings"]:
    subject = binding["s"]["value"]
    predicate = binding["p"]["value"]
    obj = binding["o"]["value"]

    # Print each triple in a more readable format
    print(f"Subject: {subject}")
    print(f"Predicate: {predicate}")
    print(f"Object: {obj}")
    print("-" * 40)  # Separator between triples


# Test 2: Add Triples

# Define the triples to be added
triples_to_add = [
    ("http://example.com/omikb#Subject1", "http://example.com/omikb#Predicate1", "http://example.com/omikb#Object1"),
    ("http://example.com/omikb#Subject2", "http://example.com/omikb#Predicate2", Literal("Object2", lang="en")),
    ("http://example.com/omikb#Subject3", "http://example.com/omikb#Predicate3", "http://example.com/omikb#Object3"),
]

# Add the triples to the triplestore
try:
    result = ts.add_triples(triples_to_add)
    print("Triples added successfully. Result:", result)
except Exception as e:
    print(f"Error adding triples: {e}")

# Verify that the triples were added by querying
verification_query = """
SELECT ?s ?p ?o WHERE {
    { ?s ?p ?o }
}
LIMIT 10
"""

print(f"Executing verification query: {verification_query}")

# Execute the verification query
try:
    verification_result = ts.query(verification_query)
    print("Verification Query Results:", verification_result)
except Exception as e:
    print(f"Verification query execution failed: {e}")



# Test 3: Parse an Ontology


ts.parse("http://www.w3.org/People/Berners-Lee/card")
# # Query to check if FOAF data exists in the triplestore


sparql_query = """
SELECT ?s ?p ?o
WHERE {
  ?s ?p 'Tim Berners-Lee'
}
LIMIT 100
"""
sparql_query = "SELECT * WHERE { ?s ?p ?o } LIMIT 1000"

print(f"Executing query: {sparql_query}")

# Execute the query
result = ts.query(sparql_query)

# Lines to make the query results readable

print("Query Results:")
for binding in result["results"]["bindings"]:
    subject = binding["s"]["value"]
    predicate = binding["p"]["value"]
    obj = binding["o"]["value"]

    # Print each triple in a more readable format
    print(f"Subject: {subject}")
    print(f"Predicate: {predicate}")
    print(f"Object: {obj}")
    print("-" * 40)  # Separator between triples
