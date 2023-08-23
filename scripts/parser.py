# script to load and parse RDF data using the rdflib library.
# Import the rdflib library
from rdflib import Graph

def parse_rdf_file(rdf_file_path):
    # Load RDF data from a file in Turtle format
    g = Graph()
    g.parse(rdf_file_path, format='turtle')

    # Iterate over the parsed RDF triples
    for s, p, o in g:
        # s: subject, p: predicate, o: object
        print(f"Subject: {s}, Predicate: {p}, Object: {o}")

if __name__ == "__main__": #run the code inside when executing the script directly
    rdf_file_path = 'data.rdf' #remember to have data.rdf file is in the same directory
    parse_rdf_file(rdf_file_path)

#if you run python rdf_parser.py in the command line, it will parse the RDF data in data.rdf and print the triples