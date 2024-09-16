from tripper import Triplestore
localhost = "http://localhost:3030"
database = "openmodel"
import os
# Initialize the knowledge base
__triplestore_url = os.getenv("TRIPLESTORE_URL", localhost)
ts = Triplestore(backend="fuseki", triplestore_url=__triplestore_url, database=database)
ts.bind("emmo", "https://w3id.org/emmo#")
ts.bind("ss3", "http://open-model.eu/ontologies/ss3#")


ts.remove_database(
    backend="fuseki", triplestore_url=__triplestore_url, database=database
)

print('Parsing food.ttl')
ts.parse(os.path.abspath('../ontologies/food.ttl'), format="turtle")
#ts_fuseki.parse("temp.ttl", format="turtle")
print('adding triple')
ts.add_triples(('a','b','c'))


