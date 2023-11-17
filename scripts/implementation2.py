#!/usr/bin/env python
# coding: utf-8

# In[489]:


from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import FOAF, DC, XSD
from SPARQLWrapper import SPARQLWrapper, POST, JSON
from urllib.parse import urlparse, urlunparse, quote
import pandas as pd
import json
import csv
from typing import List, Set
from IPython.display import display
import urllib.parse




class IdentifiableEntity:
    def __init__(self, id):
        self.ids = id if isinstance(id, list) else [id]

    def getIds(self):
        return self.ids

class Person(IdentifiableEntity):
    def __init__(self, id, given_name, family_name):
        super().__init__(id)
        self.given_name = given_name
        self.family_name = family_name

    def getGivenName(self):
        return self.given_name

    def getFamilyName(self):
        return self.family_name

 
    def getOrcid(self):
        
        return self.ids[0] if self.ids else None

class Publication(IdentifiableEntity):
    def __init__(self, id, title, publication_year=None, venue=None, authors=None, publisher=None):
        super().__init__(id)
        self.title = title
        self.publicationYear = publication_year
        self.publicationVenue = venue 
        self.authors = set(authors) if authors else set()
        self.citedPublications = []
        self.publisher = publisher  # Add this line to include the publisher attribute


    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getCitedPublications(self):
        return self.citedPublications

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        return self.authors

    def getPublisher(self): 
        return self.publisher

    def getVenueType(self):
        if isinstance(self.publicationVenue, Venue):
            return self.publicationVenue.getVenueType()
        return None

class Venue(IdentifiableEntity):
    def __init__(self, id, title):
        super().__init__(id)
        self.title = title
        self.publisher = None

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher

    def setPublisher(self, publisher):
        if isinstance(publisher, Organization):
            self.publisher = publisher
            
    def getVenueType(self):
        return "generic"  


class ProceedingsPaper(Publication):
    def __init__(self, id, title, publication_year, publication_venue=None):
        super().__init__(id, title, publication_year, publication_venue)

class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        super().__init__(id)
        self.name = name

    def getName(self):
        return self.name

class JournalArticle(Publication):
    def __init__(self, id, title, publication_year, issue=None, volume=None, publisher=None, publication_venue=None):
        super().__init__(id, title, publication_year, publication_venue, publisher=publisher)
        self.issue = issue
        self.volume = volume


    def getIssue(self):
        return self.issue

    def getVolume(self):
        return self.volume

class BookChapter(Publication):
    def __init__(self, id, title, publication_year, chapter_number, publication_venue=None, publisher_crossref=None):
        super().__init__(id, title, publication_year, publication_venue, None, publisher_crossref)
        self.chapterNumber = chapter_number

    def getChapterNumber(self):
        return self.chapterNumber

class Journal(Venue):
    def getVenueType(self):
        return "journal"

class Book(Venue):
    def getVenueType(self):
        return "book"

class Proceedings(Venue):
    def __init__(self, id, title, event):
        super().__init__(id, title)
        self.event = event

    def getEvent(self):
        return self.event

    def getVenueType(self):
        return "conference"


# In[497]:


import random


# Define your namespaces based on your RDF Schema or Ontology
SCHEMA = Namespace("http://schema.org/")
CUSTOM = Namespace("https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/")

# Inheritance example: TriplestoreQueryProcessor inherits from TriplestoreProcessor
class TriplestoreProcessor:
    def __init__(self, endpointUrl=None):  # Set a default value for endpointUrl
        self.endpointUrl = endpointUrl if endpointUrl is not None else ""
        
    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, url):
        self.endpointUrl = url
        return True


# Association example: TriplestoreDataProcessor has an association with TriplestoreProcessor
class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self, endpointUrl="http://localhost:9999/blazegraph/sparql"):
        super().__init__(endpointUrl)  
        self.sparql = SPARQLWrapper(self.endpointUrl)      
        self.base_uri = SCHEMA
        self.custom_base_uri = CUSTOM
        self.graph = Graph()
        # Initialize mappings
        self.authors_mapping = {}
        self.venues_mapping = {}
        self.publishers_mapping = {}
        self.references_mapping = {}
        # Temporary storage for publications if JSON is not yet loaded
        self.temp_publications = []
        # Flags to check if CSV or JSON data has been loaded
        self.csv_loaded = False
        self.json_loaded = False
    '''
    def print_random_publication_details(self):
        if not self.temp_publications:
            print("No publications available.")
            return

        # Select a random publication
        random_publication = random.choice(self.temp_publications)

        # Print details
        print(f"Publication ID: {random_publication.getIds()[0]}")
        print(f"Title: {random_publication.getTitle()}")
        print(f"Publication Year: {random_publication.getPublicationYear()}")
        print(f"Venue Type: {getattr(random_publication, 'venueType', 'Not available')}")
        print(f"Publication Venue: {getattr(random_publication, 'publicationVenue', 'Not available')}")



    def serialize_and_inspect_rdf_sample(self, num_samples=5):
        sample_graph = Graph()
        for publication in self.temp_publications[:num_samples]:
            rdf_data, _ = self.convert_publication_to_rdf(publication)  # Expecting two return values
            sample_graph.parse(data=rdf_data, format='nt')

    
        # Serialize the sample graph to a string
        serialized_data = sample_graph.serialize(format='nt')
        print(serialized_data)
        return serialized_data
    '''
    def uploadData(self, path: str):
        # Check the file extension and call the appropriate loading function
        if path.lower().endswith('.csv'):
            success = self.load_csv(path)
        elif path.lower().endswith('.json'):
            success = self.load_json(path)
        else:
            raise ValueError("Unsupported file type. Please provide a .csv or .json file.")
    
        # Check if both CSV and JSON data have been loaded
        if self.csv_loaded and self.json_loaded:
            # Enrich publications with additional information
            self.enrich_publications()
    
            # Convert publications to RDF and add to the graph
            self.convert_to_rdf() 
    
            # Upload RDF data to the triplestore
            self.upload_rdf_to_triplestore()  
    
        return success

    def load_csv(self, csv_path: str):
        self.csv_data = pd.read_csv(csv_path).to_dict(orient='records')  # Store the entire CSV data
        df = pd.read_csv(csv_path)
    
        for index, row in df.iterrows():
            # Extracting all fields
            publication_id = row['id']
            title = row['title']
            publication_type = row.get('type', None)
            publication_year = row['publication_year']
            issue = row.get('issue', None)
            volume = row.get('volume', None)
            chapter_number = row.get('chapter', None)
            publisher_crossref = row.get('publisher', None)
            publication_venue = row.get('publication_venue', None)
            venue_type = row.get('venue_type', None)
            event = row.get('event', None)
    
            # Determine the type of publication and create corresponding objects
            if publication_type == 'journal-article':
                publication = JournalArticle(publication_id, title, publication_year, issue, volume, publisher_crossref, publication_venue)
            elif publication_type == 'book-chapter':
                # Pass publisher_crossref to the BookChapter constructor
                publication = BookChapter(publication_id, title, publication_year, chapter_number, publication_venue, publisher_crossref)      
            elif publication_type == 'proceedings-paper':
                publication = ProceedingsPaper(publication_id, title, publication_year, publication_venue)
            else:
                # Generic publication object for other types
                publication = Publication(publication_id, title, publication_year, publication_venue, None, publisher_crossref)

            # Set common properties
            publication.venueType = venue_type
            publication.event = event
    
            # Add the publication object to the temporary storage
            self.temp_publications.append(publication)
    
        self.csv_loaded = True
        return True

       

    def load_json(self, json_path: str):
        with open(json_path, 'r') as file:
            self.json_data = json.load(file)  # Load JSON data into an instance variable
    
        # Processing authors
        for publication_doi, authors_list in self.json_data['authors'].items():
            for author_info in authors_list:
                author_id = author_info['orcid']
                given_name = author_info['given']
                family_name = author_info['family']
                author = Person(author_id, given_name, family_name)
                self.authors_mapping[author_id] = author
    
        # Processing venues
        # Just store the mapping for now, we'll connect venues to publications later
        self.venues_mapping = self.json_data['venues_id']
    
        # Processing publishers
        for publisher_id, publisher_info in self.json_data['publishers'].items():
            publisher = Organization(publisher_info['id'], publisher_info['name'])
            self.publishers_mapping[publisher_id] = publisher
    
        # Processing references
        self.references_mapping = self.json_data['references']

        # Handling JSON-only DOIs
        json_dois = set(self.json_data['authors'].keys()) | set(self.json_data['venues_id'].keys())
        existing_dois = {pub.getIds()[0] for pub in self.temp_publications}
        json_only_dois = json_dois - existing_dois
        for doi in json_only_dois:
            title = "Title not provided"  # Placeholder title
            publication = Publication(doi, title)
            self.temp_publications.append(publication)

        # Debugging: Print publishers_mapping to verify its contents
        #for crossref, publisher in self.publishers_mapping.items():
            #print(f"Crossref ID: {crossref}, Publisher: {publisher.getName()}")

        
        self.json_loaded = True
        return True
        
            
        # If CSV data has been loaded, enrich the publication data
        if self.csv_loaded:
            self.enrich_publications()

        return True


    def enrich_publications(self):
        for publication in self.temp_publications:
            doi = publication.getIds()[0]
    
            # Debugging: Print publication type and DOI
            #print(f"Processing DOI: {doi}, Type: {type(publication).__name__}, Initial Publisher: {publication.getPublisher()}")
    
            # Connect authors based on DOI
            authors_list = [Person(author['orcid'], author['given'], author['family'])
                            for author in self.json_data['authors'].get(doi, [])]
            publication.authors.update(authors_list)
    
            # Connect venues based on DOI and store the venue ID (ISSN/ISBN)
            venue_ids = self.json_data['venues_id'].get(doi, [])
            if venue_ids:
                # Assuming the first venue ID is the primary one
                venue_id = venue_ids[0]
                venue_title = self.json_data['venues_id'].get(venue_id, {}).get('title')
                if venue_title:
                    publication.publicationVenue = venue_title
                # Store the venue ID (ISSN/ISBN) in the publication object
                publication.venueID = venue_id

    
            # Connect references based on DOI
            publication.citedPublications = self.json_data['references'].get(doi, [])
    
            # Initialize publisher_crossref variable
            publisher_crossref = None
    
            # Retrieve publisher information from publication object
            publisher_info = publication.getPublisher()
            if publisher_info:
                # Check if publisher is an Organization object (from JSON)
                if isinstance(publisher_info, Organization):
                    publisher_crossref = publisher_info.ids[0]  # Assuming the first ID is Crossref
                else:
                    # If publisher_info is already a Crossref ID (string)
                    publisher_crossref = publisher_info
    
            # If no publisher in publication object, check JSON with DOI as a key
            if not publisher_crossref:
                publisher_info_json = self.json_data['publishers'].get(doi)
                if publisher_info_json:
                    publisher_crossref = publisher_info_json['id']
    
            # Update the publication's publisher attribute to store only the Crossref ID
            publication.publisher = publisher_crossref
    
            # Debugging: Print final publisher info
            #print(f"Final Publisher for {doi}: {publication.getPublisher()}")
    
            # Additional logic for ProceedingsPaper
            if isinstance(publication, ProceedingsPaper) and hasattr(publication, 'event'):
                publication.event = self.json_data['events'].get(doi, 'Unknown Event')
    
            # Attempt to fill in missing titles with data from the CSV if loaded
            if self.csv_loaded and not publication.getTitle():
                csv_entry = next((row for row in self.csv_data if row['id'] == doi), None)
                if csv_entry:
                    publication.title = csv_entry['title']


    def sanitize_uri(self, uri: str) -> str:
        """
        Ensures that the URI is valid and properly encoded for RDF and SPARQL usage.
        Also handles DOIs, ORCIDs, ISSNs, ISBNs, and Crossref IDs by appending appropriate resolver URLs.
        """
        # Prepend the DOI resolver URL if it's a DOI
        if uri.startswith('doi:'):
            uri = 'https://doi.org/' + uri[4:]  # Remove the 'doi:' part and prepend the resolver URL
        
        # Prepend the ORCID URL if it's an ORCID identifier
        elif uri.startswith('0000-'):
            uri = 'https://orcid.org/' + uri
        
        # Handle venue names or other arbitrary strings
        parsed_uri = urlparse(uri)
        if not (parsed_uri.scheme and parsed_uri.netloc):
            # URL encode the string to make it a valid URI
            encoded_uri = quote(uri, safe='')
            # Prepend the SCHEMA namespace as the base URI
            uri = str(SCHEMA[encoded_uri])
        
        # Prepend a base URI for ISSNs
        elif uri.startswith('issn:'):
            uri = 'http://purl.org/issn/' + uri[5:]  # Remove the 'issn:' part and prepend the base URI
        
        # Prepend a base URI for ISBNs
        elif uri.startswith('isbn:'):
            uri = 'https://www.isbn-international.org/identifier/' + uri[5:]  # Remove the 'isbn:' part and prepend the base URI
        
        # Prepend a base URI for Crossref IDs
        elif uri.startswith('crossref:'):
            crossref_id = uri[9:]  # Extract the Crossref ID
            # Directly build the URI without double encoding
            uri = 'https://api.crossref.org/works/' + quote(crossref_id)
        
        # Check if a valid URI is already formed
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme and parsed_uri.netloc:
            # URI is valid, proceed to encode the components
            encoded_path = quote(parsed_uri.path, safe='/')
            encoded_query = quote(parsed_uri.query, safe='=&?/')
            encoded_fragment = quote(parsed_uri.fragment, safe='')
        
            # Rebuild the URI with the encoded components
            return urlunparse((
                parsed_uri.scheme,
                parsed_uri.netloc,
                encoded_path,
                parsed_uri.params,
                encoded_query,
                encoded_fragment
            ))
        else:
            # If the scheme and netloc are missing after the checks, the URI cannot be validated
            raise ValueError(f"Invalid URI: {uri}")
            
        
    def convert_publication_to_rdf(self, publication):
        # Create a new graph for the individual publication
        graph = Graph()
        pub_uri = URIRef(self.sanitize_uri(str(publication.getIds()[0])))
        
        # Add common properties with schema.org vocabulary
        graph.add((pub_uri, RDF.type, SCHEMA.ScholarlyArticle))
        graph.add((pub_uri, SCHEMA.name, Literal(publication.getTitle())))
    
        # Add the publication year, only if it's not None
        if publication.getPublicationYear() is not None:
            graph.add((pub_uri, SCHEMA.datePublished, Literal(str(publication.getPublicationYear()), datatype=XSD.gYear)))
            
        # Add authors to the graph
        for author in publication.getAuthors():
            author_uri = URIRef(self.sanitize_uri(author.getOrcid()))
            graph.add((author_uri, RDF.type, SCHEMA.Person))
            graph.add((author_uri, SCHEMA.givenName, Literal(author.getGivenName())))
            graph.add((author_uri, SCHEMA.familyName, Literal(author.getFamilyName())))
            graph.add((pub_uri, SCHEMA.author, author_uri))
        
        # Handle publisher information
        publisher_crossref = publication.getPublisher()
        if publisher_crossref and publisher_crossref in self.publishers_mapping:
            # Decode the Crossref ID
            decoded_crossref = urllib.parse.unquote(publisher_crossref)
            publisher = self.publishers_mapping[decoded_crossref]
            publisher_uri = URIRef(self.sanitize_uri(decoded_crossref)) 
            # Get the publisher object from the mapping
            publisher = self.publishers_mapping[publisher_crossref]
            #print(f"Found publisher for {publisher_crossref}: {publisher.getName()}")  # Debugging statement
            publisher_uri = URIRef(self.sanitize_uri(publisher_crossref))  # Sanitize the Crossref ID
            # Add RDF triples for the publisher
            graph.add((publisher_uri, RDF.type, SCHEMA.Organization))
            graph.add((publisher_uri, SCHEMA.name, Literal(publisher.getName())))
            # Link the publication to its publisher
            graph.add((pub_uri, SCHEMA.publisher, publisher_uri))
        

        # Handle venue type information
        venue_type = getattr(publication, 'venueType', None)
        if venue_type:
            graph.add((pub_uri, CUSTOM.venueType, Literal(venue_type)))  # Add venue type as a literal

        # Handle venue ID (ISSN/ISBN)
        if hasattr(publication, 'venueID') and publication.venueID:
            venue_uri = URIRef(self.sanitize_uri(publication.venueID))
            graph.add((venue_uri, RDF.type, SCHEMA.Periodical))  # Or appropriate type
            graph.add((pub_uri, SCHEMA.isPartOf, venue_uri))  # Link publication to its venue
        
        # Handle publication venue information correctly
        pub_venue = publication.getPublicationVenue()
        if pub_venue:
            # You can either add it as a URI (if it's a resolvable URL) or as a literal
            # To add as a literal:
            graph.add((pub_uri, CUSTOM.publicationVenue, Literal(pub_venue)))
        
        # Handle specific properties based on publication type
        if isinstance(publication, JournalArticle):
            # Add journal-specific properties such as volume and issue
            if publication.getVolume():
                graph.add((pub_uri, SCHEMA.volumeNumber, Literal(publication.getVolume())))
            if publication.getIssue():
                graph.add((pub_uri, SCHEMA.issueNumber, Literal(publication.getIssue())))
    
        elif isinstance(publication, BookChapter):
            # Add book chapter-specific properties such as chapter number
            if publication.getChapterNumber() is not None:
                graph.add((pub_uri, SCHEMA.chapterNumber, Literal(publication.getChapterNumber())))
    
        elif isinstance(publication, ProceedingsPaper):
            # Add proceedings-specific properties (e.g., event)
            if publication.getEvent() is not None:
                graph.add((pub_uri, SCHEMA.event, Literal(publication.getEvent())))
    
        # Add cited publications
        for cited_doi in publication.getCitedPublications():
            cited_uri = URIRef(self.sanitize_uri(cited_doi))
            graph.add((pub_uri, SCHEMA.citation, cited_uri))
    
        # Serialize the individual publication graph to N-Triples
        rdf_data = graph.serialize(format='nt')
        triple_count = len(graph)
        return rdf_data, triple_count


    def save_graph_to_file(self, graph, file_path):
        # Serialize the entire graph to N-Triples
        rdf_data = graph.serialize(format='nt')
        
        # Save to a text file
        with open(file_path, 'w') as file:
            file.write(rdf_data)
    
        print(f"Saved RDF data to {file_path}")

        
    def convert_to_rdf(self):
        """
        Converts the enriched publication objects to RDF triples and adds them to the graph.
        """
        for publication in self.temp_publications:
            # Use the convert_publication_to_rdf method to convert each publication
            rdf_data, triple_count = self.convert_publication_to_rdf(publication)
            # Parse the serialized RDF data into the main graph
            self.graph.parse(data=rdf_data, format='nt')
        
        # Print the total number of triples for verification
        #print(f"Total number of triples in the graph: {len(self.graph)}")

        # Once the RDF graph is complete, upload it to the triplestore
        self.upload_rdf_to_triplestore()



    def upload_rdf_to_triplestore(self):
        try:
            # Serialize the graph in NTriples format
            serialized_graph = self.graph.serialize(format='nt')

            # Print out the serialized graph to inspect it
            #print(serialized_graph)

            # Initialize SPARQLWrapper with the endpoint URL
            sparql = SPARQLWrapper('http://localhost:9999/blazegraph/sparql')

            # Set the request method to POST
            sparql.setMethod(POST)

            # Set the query for SPARQLWrapper
            sparql.setQuery(f'INSERT DATA {{ {serialized_graph} }}')

            # Perform the query
            response = sparql.query()

            # Handle the response
            #print(f"Response from the triplestore: {response.response.read()}")

        except Exception as e:
            print(f"An error occurred during RDF upload: {e}")



class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self, endpointUrl=None):
        super().__init__(endpointUrl)
        self.sparql = SPARQLWrapper(self.endpointUrl if self.endpointUrl else "http://localhost:9999/blazegraph/sparql")
        
    def run_query(self, query):
        try:
            self.sparql.setQuery(query)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()

            # Process the results to have cleaner column names
            processed_results = []
            for result in results["results"]["bindings"]:
                processed_row = {}
                for key, value in result.items():
                    processed_row[key] = value.get('value', None)  # Get the 'value' for each key
                processed_results.append(processed_row)

            return pd.DataFrame(processed_results)
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    @staticmethod
    def format_identifier(identifier):
        if identifier.startswith("issn:"):
            formatted_identifier = identifier.replace("issn:", "issn%253A")
        elif identifier.startswith("isbn:"):
            formatted_identifier = identifier.replace("isbn:", "isbn%253A")
        else:
            formatted_identifier = identifier  # Or handle other cases as needed
        return formatted_identifier

    @staticmethod
    def format_dois(dois):
        if not isinstance(dois, list):
            dois = [dois]
    
        formatted_dois = []
        for doi in dois:
            if doi.startswith("doi:"):
                formatted_doi = f"<https://doi.org/{doi[4:]}>"
            elif "https://doi.org/" not in doi:
                formatted_doi = f"<https://doi.org/{doi}>"
            else:
                formatted_doi = f"<{doi}>"
            formatted_dois.append(formatted_doi)
    
        return formatted_dois

    @staticmethod
    def format_crossref_publisher_id(publisher_id):
        if publisher_id.startswith("crossref:"):
            formatted_id = publisher_id.split(':')[1]
        else:
            formatted_id = publisher_id  # Or handle other cases as needed
        return f"https://api.crossref.org/works/{formatted_id}"
    
    def getPublicationsPublishedInYear(self, year):  # Renamed method
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (GROUP_CONCAT(DISTINCT ?publicationVenue; separator=", ") AS ?publicationVenues) 
               (GROUP_CONCAT(DISTINCT CONCAT(?givenName, " ", ?familyName); separator=", ") AS ?authors)
               (GROUP_CONCAT(DISTINCT STR(?citedPublication); separator=", ") AS ?citedPublications)
               (SAMPLE(?publisherName) AS ?publisherNames) (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?venueType) AS ?venueTypes) (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) (SAMPLE(?chapter) AS ?chapterNumbers) 
               (SAMPLE(?event) AS ?events)
        WHERE {{
          ?id schema:datePublished "{year}"^^<http://www.w3.org/2001/XMLSchema#gYear> .
          ?id schema:name ?title .
        
          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}
        
          OPTIONAL {{
            ?id schema:author ?author .
            ?author schema:familyName ?familyName .
            ?author schema:givenName ?givenName .
          }}
        
          OPTIONAL {{
            ?id schema:citation ?citedPublication .
          }}
        
          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}
        
          OPTIONAL {{ ?id schema:issueNumber ?issue . }}
          OPTIONAL {{ ?id schema:volumeNumber ?volume . }}
          OPTIONAL {{ ?id schema:chapterNumber ?chapter . }}
          OPTIONAL {{ ?id schema:event ?event . }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)

    def getPublicationsByAuthorId(self, author_id):  # Renamed method
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (GROUP_CONCAT(DISTINCT ?publicationVenue; separator=", ") AS ?publicationVenues) 
               (GROUP_CONCAT(DISTINCT CONCAT(?givenName, " ", ?familyName); separator=", ") AS ?authors)
               (GROUP_CONCAT(DISTINCT STR(?citedPublication); separator=", ") AS ?citedPublications)
               (SAMPLE(?publisherName) AS ?publisherNames) (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?venueType) AS ?venueTypes) (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) (SAMPLE(?chapter) AS ?chapterNumbers) 
               (SAMPLE(?event) AS ?events)
        WHERE {{
          ?id schema:author <https://orcid.org/{author_id}> .
          ?id schema:name ?title .
        
          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}
        
          OPTIONAL {{
            ?id schema:author ?author .
            ?author schema:familyName ?familyName .
            ?author schema:givenName ?givenName .
          }}
        
          OPTIONAL {{
            ?id schema:citation ?citedPublication .
          }}
        
          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}
        
          OPTIONAL {{ ?id schema:issueNumber ?issue . }}
          OPTIONAL {{ ?id schema:volumeNumber ?volume . }}
          OPTIONAL {{ ?id schema:chapterNumber ?chapter . }}
          OPTIONAL {{ ?id schema:event ?event . }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)

    def getMostCitedPublication(self):  # Renamed method
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (COUNT(DISTINCT ?citedBy) AS ?citationCount)
               (GROUP_CONCAT(DISTINCT ?publicationVenue; separator=", ") AS ?publicationVenues) 
               (GROUP_CONCAT(DISTINCT CONCAT(?givenName, " ", ?familyName); separator=", ") AS ?authors)
               (SAMPLE(?publisherName) AS ?publisherNames) (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?venueType) AS ?venueTypes) (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) (SAMPLE(?chapter) AS ?chapterNumbers) 
               (SAMPLE(?event) AS ?events)
        WHERE {
          ?id schema:name ?title .

          OPTIONAL { ?id custom:publicationVenue ?publicationVenue . }
          OPTIONAL { ?id custom:venueType ?venueType . }

          OPTIONAL {
            ?id schema:author ?author .
            ?author schema:familyName ?familyName .
            ?author schema:givenName ?givenName .
          }

          OPTIONAL {
            ?citedBy schema:citation ?id .
          }

          OPTIONAL {
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }

          OPTIONAL { ?id schema:issueNumber ?issue . }
          OPTIONAL { ?id schema:volumeNumber ?volume . }
          OPTIONAL { ?id schema:chapterNumber ?chapter . }
          OPTIONAL { ?id schema:event ?event . }
        }
        GROUP BY ?id ?title
        ORDER BY DESC(?citationCount)
        LIMIT1
        """
        return self.run_query(query)

    def getMostCitedVenue(self):
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?venue
               (COUNT(DISTINCT ?citedBy) AS ?totalCitations)
               (SAMPLE(?venueType) AS ?venueTypes)
               (SAMPLE(?publisherName) AS ?publisherNames)
               (SAMPLE(?crossrefID) AS ?crossrefIDs)
        WHERE {
          ?publication custom:publicationVenue ?venue .
          OPTIONAL { ?publication custom:venueType ?venueType . }
          
          OPTIONAL {
            ?citedBy schema:citation ?publication .
          }

          OPTIONAL {
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }
        }
        GROUP BY ?venue
        ORDER BY DESC(?totalCitations)
        LIMIT1
        """
        return self.run_query(query)

    def getVenuesByPublisherId(self, publisher_id):
        formatted_publisher_id = self.format_crossref_publisher_id(publisher_id)
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?venue
        WHERE {{
          ?publication schema:publisher <{formatted_publisher_id}> .
          ?publication schema:isPartOf ?venue .
        }}
        """
        return self.run_query(query)

    def getPublicationInVenue(self, venue_identifier):
        formatted_venue_identifier = self.format_identifier(venue_identifier)  # Format the identifier
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (GROUP_CONCAT(DISTINCT ?publicationVenue; separator=", ") AS ?publicationVenues) 
               (GROUP_CONCAT(DISTINCT CONCAT(?givenName, " ", ?familyName); separator=", ") AS ?authors)
               (GROUP_CONCAT(DISTINCT STR(?citedPublication); separator=", ") AS ?citedPublications)
               (SAMPLE(?publisherName) AS ?publisherNames) (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?venueType) AS ?venueTypes) (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) (SAMPLE(?chapter) AS ?chapterNumbers) 
               (SAMPLE(?event) AS ?events)
        WHERE {{
          ?id custom:publicationVenue ?publicationVenue .
          ?id schema:isPartOf <http://schema.org/{formatted_venue_identifier}> .
          ?id schema:name ?title .
        
          OPTIONAL {{ ?id custom:venueType ?venueType . }}
        
          OPTIONAL {{
            ?id schema:author ?author .
            ?author schema:familyName ?familyName .
            ?author schema:givenName ?givenName .
          }}
        
          OPTIONAL {{
            ?id schema:citation ?citedPublication .
          }}
        
          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}
        
          OPTIONAL {{ ?id schema:issueNumber ?issue . }}
          OPTIONAL {{ ?id schema:volumeNumber ?volume . }}
          OPTIONAL {{ ?id schema:chapterNumber ?chapter . }}
          OPTIONAL {{ ?id schema:event ?event . }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)

    def getJournalArticlesInIssue(self, identifier, volume, issue):
        formatted_identifier = self.format_identifier(identifier)  # Format the ISSN or ISBN
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (SAMPLE(?publicationYear) AS ?publicationYears)
               (SAMPLE(?publisherName) AS ?publisherNames) 
               (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?publicationVenue) AS ?publicationVenues)
               (SAMPLE(?venueType) AS ?venueTypes)
        WHERE {{
          ?id schema:isPartOf <http://schema.org/{formatted_identifier}> .
          ?id schema:volumeNumber "{volume}"^^<http://www.w3.org/2001/XMLSchema#string> .
          ?id schema:issueNumber "{issue}"^^<http://www.w3.org/2001/XMLSchema#string> .
          ?id schema:name ?title .
        
          OPTIONAL {{ ?id schema:datePublished ?publicationYear . }}
          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}
        
          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)


    def getJournalArticlesInVolume(self, identifier, volume):
        formatted_identifier = self.format_identifier(identifier)  # Format the ISSN or ISBN
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (SAMPLE(?publicationYear) AS ?publicationYears)
               (SAMPLE(?publisherName) AS ?publisherNames) 
               (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?publicationVenue) AS ?publicationVenues)
               (SAMPLE(?venueType) AS ?venueTypes)
        WHERE {{
          ?id schema:isPartOf <http://schema.org/{formatted_identifier}> .
          ?id schema:volumeNumber "{volume}"^^<http://www.w3.org/2001/XMLSchema#string> .
          ?id schema:name ?title .
        
          OPTIONAL {{ ?id schema:datePublished ?publicationYear . }}
          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}
        
          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)

    def getJournalArticlesInJournal(self, identifier):
        formatted_identifier = self.format_identifier(identifier)  # Format the ISSN or ISBN
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        
        SELECT ?id ?title 
               (SAMPLE(?publicationYear) AS ?publicationYears)
               (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) 
               (SAMPLE(?publisherName) AS ?publisherNames) 
               (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?publicationVenue) AS ?publicationVenues)
               (SAMPLE(?venueType) AS ?venueTypes)
        WHERE {{
          ?id schema:isPartOf <http://schema.org/{formatted_identifier}> .
          ?id schema:name ?title .
        
          OPTIONAL {{ ?id schema:datePublished ?publicationYear . }}
          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}
        
          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}

          OPTIONAL {{ ?id schema:issueNumber ?issue . }}
          OPTIONAL {{ ?id schema:volumeNumber ?volume . }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)

    def getProceedingsByEvent(self, eventName):
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?id ?title 
               (SAMPLE(?publicationYear) AS ?publicationYears)
               (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) 
               (SAMPLE(?publisherName) AS ?publisherNames) 
               (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?publicationVenue) AS ?publicationVenues)
               (SAMPLE(?venueType) AS ?venueTypes)
        WHERE {{
          ?id schema:event ?event .
          FILTER CONTAINS(LCASE(STR(?event)), LCASE(STR("{eventName}")))
          ?id schema:name ?title .

          OPTIONAL {{ ?id schema:datePublished ?publicationYear . }}
          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}

          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)


    def getPublicationAuthors(self, doi):
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>

        SELECT ?author ?givenName ?familyName
        WHERE {{
          <https://doi.org/{doi}> schema:author ?author .
          OPTIONAL {{
            ?author schema:givenName ?givenName .
            ?author schema:familyName ?familyName .
          }}
        }}
        """
        return self.run_query(query)


    def getPublicationsByAuthorName(self, name):
        # Convert the input name to lowercase
        lowercase_name = name.lower()

        # Construct the SPARQL query
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>

        SELECT ?id ?title 
               (GROUP_CONCAT(DISTINCT ?publicationVenue; separator=", ") AS ?publicationVenues) 
               (GROUP_CONCAT(DISTINCT CONCAT(?givenName, " ", ?familyName); separator=", ") AS ?authors)
               (GROUP_CONCAT(DISTINCT STR(?citedPublication); separator=", ") AS ?citedPublications)
               (SAMPLE(?publisherName) AS ?publisherNames) (SAMPLE(?crossrefID) AS ?crossrefIDs)
               (SAMPLE(?venueType) AS ?venueTypes) (SAMPLE(?issue) AS ?issues) 
               (SAMPLE(?volume) AS ?volumes) (SAMPLE(?chapter) AS ?chapterNumbers) 
               (SAMPLE(?event) AS ?events)
        WHERE {{
          ?id schema:author ?author .
          ?author schema:familyName ?familyName .
          FILTER(LCASE(STR(?familyName)) = "{lowercase_name}")

          ?id schema:name ?title .

          OPTIONAL {{ ?id custom:publicationVenue ?publicationVenue . }}
          OPTIONAL {{ ?id custom:venueType ?venueType . }}

          OPTIONAL {{
            ?author schema:givenName ?givenName .
          }}

          OPTIONAL {{
            ?id schema:citation ?citedPublication .
          }}

          OPTIONAL {{
            ?id schema:publisher ?publisher .
            ?publisher schema:name ?publisherName .
            BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?crossrefID)
          }}

          OPTIONAL {{ ?id schema:issueNumber ?issue . }}
          OPTIONAL {{ ?id schema:volumeNumber ?volume . }}
          OPTIONAL {{ ?id schema:chapterNumber ?chapter . }}
          OPTIONAL {{ ?id schema:event ?event . }}
        }}
        GROUP BY ?id ?title
        """
        return self.run_query(query)

    def getPublicationAuthors(self, doi):
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX custom: <https://github.com/AlessandraTrenchi/DS-pablo-ale/storesdb/>

        SELECT ?author ?givenName ?familyName
        WHERE {{
          <https://doi.org/{doi}> schema:author ?author .
          OPTIONAL {{
            ?author schema:givenName ?givenName .
            ?author schema:familyName ?familyName .
          }}
        }}
        """
        return self.run_query(query)

    def getDistinctPublisherOfPublications(self, dois):
        formatted_dois = " ".join([f"<https://doi.org/{doi}>" for doi in dois])
        query = f"""
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?publisherID ?publisherName
        WHERE {{
          VALUES ?publication {{ {formatted_dois} }}

          ?publication schema:publisher ?publisher .
          ?publisher schema:name ?publisherName .
          BIND(REPLACE(STRAFTER(STR(?publisher), "schema.org/"), "%253A", ":") AS ?publisherID)
        }}
        """
        return self.run_query(query)

        


# In[498]:


'''

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None) 
# Create an instance of the TriplestoreQueryProcessor class
query_processor = TriplestoreQueryProcessor()


# Call the getPublicationsPublishedInYear method with the year 2018
results = query_processor.getPublicationsPublishedInYear(2018)
# Print the results
display(results)

query_processor = TriplestoreQueryProcessor()
orcid_results = query_processor.getPublicationsByAuthorId("0000-0002-7383-4634")
display(orcid_results)

most_cited_results = query_processor.getMostCitedPublication()
display(most_cited_results)

most_cited_venue_results = query_processor.getMostCitedVenue()
display(most_cited_venue_results)

most_cited_publication_in_venue = query_processor.getMostCitedPublicationInVenue()
print(most_cited_publication_in_venue)

venue_results_issn = query_processor.getPublicationInVenue("issn:1061-4036")
display(venue_results_issn)

journal_issue_results = query_processor.getJournalArticlesInIssue("issn:1061-4036", "50", "4")
display(journal_issue_results)

# Using ISSN for Journal Articles in a Volume
articles_in_volume_results = query_processor.getJournalArticlesInVolume("issn:1061-4036", "50")
display(articles_in_volume_results)

# Using ISSN for Journal Articles in a Journal
articles_in_journal_results = query_processor.getJournalArticlesInJournal("issn:1061-4036")
display(articles_in_journal_results)

query_processor = TriplestoreQueryProcessor()
proceedings_results = query_processor.getProceedingsByEvent("web")
print(proceedings_results)
'''


query_processor = TriplestoreQueryProcessor()
publications_results = query_processor.getPublicationsByAuthorName("peroni")
display(publications_results)

'''
authors_results = query_processor.getPublicationAuthors("10.1007/978-3-030-00461-3_6")
display(authors_results)
'''
'''
publishers_results = query_processor.getDistinctPublisherOfPublications(["10.1007/978-3-030-00461-3_6", "10.1007/s10115-017-1100-y"])
display(publishers_results)
'''


# In[492]:


import pandas as pd
import json
import sqlite3

class RelationalProcessor:
    def __init__(self, db_path=None):
        self.db_path = db_path
        self.connection = self.create_connection() if db_path else None

    def get_db_path(self):
        return self.db_path

    def setDbPath(self, db_path):
        self.db_path = db_path
        self.connection = self.create_connection()
        self.initialize_database()  # Initialize database after setting path

    def initialize_database(self):
        # Assuming RelationalDataProcessor has the methods `clear_tables` and `create_tables`
        processor = RelationalDataProcessor()
        processor.db_processor = self  # Set current instance as db_processor
        processor.clear_tables()  # Clear tables
        processor.create_tables()  # Create tables

    def create_connection(self):
        if self.db_path:
            # Create and return the database connection
            return sqlite3.connect(self.db_path)
        else:
            return None

class RelationalDataProcessor:
    def __init__(self):
        self.db_processor = RelationalProcessor()  # Create a new RelationalProcessor instance with no db_path
        self.csv_loaded = False
        self.json_loaded = False
        self.temp_publications = []

    # This method will be called directly after setting the database path
    def initialize_database(self):
        self.clear_tables()  # Clear tables
        self.create_tables()  # Create tables
    def clear_tables(self):
        with self.db_processor.create_connection() as conn:
            # Clear all relevant tables before loading new data
            tables_to_clear = ["csv_data", "publication_references", 
                               "publications", "venues", "publishers", 
                               "authors"]
            for table in tables_to_clear:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
                    conn.commit()  # Ensure changes are committed
                except Exception as e:
                    print(f"An error occurred while dropping {table}: {e}")

    def create_tables(self):
    # Connect to the SQLite database
        with self.db_processor.create_connection() as conn:
            # Create the publications table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS publications (
                id TEXT PRIMARY KEY,
                title TEXT,
                publication_year INTEGER,
                issue TEXT,
                volume TEXT,
                chapter_number TEXT,
                publisher_crossref TEXT,
                publication_venue TEXT,
                event TEXT,
                type TEXT,
                authors TEXT,
                venue_id TEXT,
                cited_publications TEXT,
                publisher_id TEXT
            );
            """)
    
            # Create the authors table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS authors (
                orcid TEXT,
                given_name TEXT,
                family_name TEXT,
                publication_doi TEXT
            );
            """)
    
            # Modify the venues table to include ISSN and ISBN
            conn.execute("""
            CREATE TABLE IF NOT EXISTS venues (
                id TEXT,
                publication_doi TEXT,
                issn TEXT,
                isbn TEXT
            );
            """)
    
            # Create the publishers table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS publishers (
                id TEXT,
                name TEXT,
                crossref TEXT
            );
            """)
    
            # Rename references table to avoid using a reserved keyword
            conn.execute("""
            CREATE TABLE IF NOT EXISTS publication_references (
                publication_doi TEXT,
                referenced_doi TEXT
            );
            """)

            # Create the csv_data table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS csv_data (
                id TEXT PRIMARY KEY,
                title TEXT,
                publication_year INTEGER,
                issue TEXT,
                volume TEXT,
                chapter_number TEXT,
                publisher_crossref TEXT,
                publication_venue TEXT,
                event TEXT,
                type TEXT
            );
            """)
    
            conn.commit()


    def uploadData(self, path: str):
        # Check the file extension and call the appropriate loading function
        if path.lower().endswith('.csv'):
            success = self.load_csv(path)
        elif path.lower().endswith('.json'):
            success = self.load_json(path)
        else:
            raise ValueError("Unsupported file type. Please provide a .csv or .json file.")

        # If both CSV and JSON data have been loaded, upload data to the relational database
        if self.csv_loaded and self.json_loaded:
            self.upload_data_to_db()
        return success

    def load_csv(self, csv_path: str):
        df = pd.read_csv(csv_path)
        with self.db_processor.create_connection() as conn:
            # Clear existing data in the tables
            conn.execute("DELETE FROM csv_data")
            conn.execute("DELETE FROM publications")  # Clear publications table
    
            for index, row in df.iterrows():
                # Insert into csv_data table
                csv_sql = """
                INSERT INTO csv_data (id, title, publication_year, issue, volume, chapter_number, publisher_crossref, publication_venue, event, type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                csv_data = (
                    row['id'],
                    row['title'],
                    row['publication_year'],
                    row.get('issue'),
                    row.get('volume'),
                    row.get('chapter'),
                    row.get('publisher'),
                    row.get('publication_venue'),
                    row.get('event'),
                    row['type']
                )
                conn.execute(csv_sql, csv_data)
    
                # Also, insert into publications table
                # Adjust the columns and data as per your publications table schema
                pub_sql = """
                INSERT INTO publications (id, title, publication_year, issue, volume, chapter_number, publisher_crossref, publication_venue, event, type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                pub_data = (
                    row['id'],
                    row['title'],
                    row['publication_year'],
                    row.get('issue'),
                    row.get('volume'),
                    row.get('chapter'),
                    row.get('publisher'),
                    row.get('publication_venue'),
                    row.get('event'),
                    row['type']
                )
                conn.execute(pub_sql, pub_data)
    
        self.csv_loaded = True
        return True

    
    def load_json(self, json_path: str):
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        
        with self.db_processor.create_connection() as conn:
            # Process authors
            for doi, authors in json_data['authors'].items():
                for author in authors:
                    sql = "INSERT INTO authors (orcid, given_name, family_name, publication_doi) VALUES (?, ?, ?, ?)"
                    data = (author['orcid'], author['given'], author['family'], doi)
                    conn.execute(sql, data)
    
            # Process venues
            for doi, venue_ids in json_data['venues_id'].items():
                for venue_id in venue_ids:
                    # Check if it's ISSN or ISBN and insert accordingly
                    issn = venue_id if venue_id.startswith('issn:') else None
                    isbn = venue_id if venue_id.startswith('isbn:') else None
    
                    sql = "INSERT INTO venues (id, publication_doi, issn, isbn) VALUES (?, ?, ?, ?)"
                    data = (None, doi, issn, isbn)  # 'id' can be None or some identifier if needed
                    conn.execute(sql, data)
    
            # Process publishers
            for crossref, publisher in json_data['publishers'].items():
                sql = "INSERT INTO publishers (id, name, crossref) VALUES (?, ?, ?)"
                data = (publisher['id'], publisher['name'], crossref)
                conn.execute(sql, data)
    
            # Process publication references
            for doi, references in json_data['references'].items():
                for reference in references:
                    sql = "INSERT INTO publication_references (publication_doi, referenced_doi) VALUES (?, ?)"
                    data = (doi, reference)
                    conn.execute(sql, data)
    
        self.json_loaded = True
        return True

    def enrich_publications(self):
        with self.db_processor.create_connection() as conn:
            # Connect authors based on DOI
            sql = """
            UPDATE publications
            SET authors = (
                SELECT GROUP_CONCAT(given_name || ' ' || family_name) 
                FROM authors 
                WHERE publication_doi = publications.id
            )
            """
            conn.execute(sql)
            
            # Connect venues based on DOI
            sql = """
            UPDATE publications
            SET venue_id = (SELECT id FROM venues WHERE publication_doi = publications.id LIMIT 1)
            """
            conn.execute(sql)
            
            # Connect references based on DOI
            sql = """
            UPDATE publications
            SET cited_publications = (
                SELECT GROUP_CONCAT(referenced_doi) 
                FROM publication_references 
                WHERE publication_doi = publications.id
            )
            """
            conn.execute(sql)
            
            # Update titles where necessary
            sql = """
            UPDATE publications
            SET title = (
                SELECT title FROM csv_data WHERE id = publications.id AND title IS NOT NULL
            )
            WHERE title = 'Title not provided'
            """
            conn.execute(sql)
            
            # Connect publisher based on Crossref value from CSV
            sql = """
            UPDATE publications
            SET publisher_id = (SELECT id FROM publishers WHERE crossref = publications.publisher_crossref)
            """
            conn.execute(sql)
            
            # Update event for ProceedingsPaper
            sql = """
            UPDATE publications
            SET event = (
                SELECT event FROM csv_data WHERE id = publications.id AND type = 'proceedings-paper' AND event IS NOT NULL
            )
            WHERE type = 'proceedings-paper' AND event IS NULL OR event = ''
            """
            conn.execute(sql)
            
            # Update titles where necessary
            sql = """
            UPDATE publications
            SET title = (
                SELECT title FROM csv_data WHERE id = publications.id AND title IS NOT NULL
            )
            WHERE title = 'Title not provided'
            """
            conn.execute(sql)

    def upload_data_to_db(self):
        with self.db_processor.create_connection() as conn:
            for publication in self.temp_publications:
                # Determine the type of publication and prepare the SQL statement and data accordingly
                if isinstance(publication, JournalArticle):
                    table = 'journal_articles'
                    columns = ['id', 'title', 'publication_year', 'issue', 'volume', 'publisher_id']  # example columns
                elif isinstance(publication, BookChapter):
                    table = 'book_chapters'
                    columns = ['id', 'title', 'publication_year', 'chapter_number', 'publisher_id']  # example columns
                elif isinstance(publication, ProceedingsPaper):
                    table = 'proceedings_papers'
                    columns = ['id', 'title', 'publication_year', 'event', 'publisher_id']  # example columns
                else:
                    table = 'publications'
                    columns = ['id', 'title', 'publication_year', 'publisher_id']  # example columns
                
                # Prepare the SQL command
                sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
                
                # Prepare data tuple for the SQL query
                data = [getattr(publication, column, None) for column in columns]
                
                # Execute the SQL command
                conn.execute(sql, data)
    
            # Committing at the end of all inserts for efficiency
            conn.commit()
    
    def prepare_data_tuple(self, publication):
        # Assuming publication object has properties named after the database columns
        if isinstance(publication, JournalArticle):
            return (publication.id, publication.title, publication.publicationYear, publication.issue, publication.volume, publication.publisher)
        elif isinstance(publication, BookChapter):
            return (publication.id, publication.title, publication.publicationYear, publication.chapterNumber, publication.publisher)
        elif isinstance(publication, ProceedingsPaper):
            return (publication.id, publication.title, publication.publicationYear, publication.event, publication.publisher)
        else:  # Generic Publication or other types
            return (publication.id, publication.title, publication.publicationYear, publication.publisher)


# In[493]:


class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self, db_path=None):
        super().__init__(db_path=db_path)

    def setDbProcessor(self, db_processor):
        self.db_processor = db_processor
        
    def getPublicationsPublishedInYear(self, year):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
            
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_venue AS publicationVenues,
            GROUP_CONCAT(a.given_name || ' ' || a.family_name, ', ') AS authors,
            citing.citing_publications AS citedPublications,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.type AS venueTypes,
            p.issue AS issues,
            p.volume AS volumes,
            p.chapter_number AS chapterNumbers,
            p.event AS events
        FROM 
            publications p
        LEFT JOIN 
            venues v ON p.id = v.publication_doi
        LEFT JOIN 
            authors a ON p.id = a.publication_doi
        LEFT JOIN 
            (SELECT 
                 pr.referenced_doi, 
                 GROUP_CONCAT(pr.publication_doi, ', ') AS citing_publications
             FROM 
                 publication_references pr
             GROUP BY 
                 pr.referenced_doi) citing ON p.id = citing.referenced_doi
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            p.publication_year = ?
        GROUP BY
            p.id, p.title, p.publication_venue, pub.name, pub.crossref, p.type, p.issue, p.volume, p.chapter_number, p.event;
        """
        df = pd.read_sql_query(query, conn, params=(year,))
        conn.close()
        return df
    
    def getPublicationsByAuthorId(self, author_id):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_venue AS publicationVenues,
            GROUP_CONCAT(a.given_name || ' ' || a.family_name, ', ') AS authors,
            citing.citing_publications AS citedPublications,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.type AS venueTypes,
            p.issue AS issues,
            p.volume AS volumes,
            p.chapter_number AS chapterNumbers,
            p.event AS events
        FROM 
            publications p
        LEFT JOIN 
            authors a ON p.id = a.publication_doi
        LEFT JOIN 
            (SELECT 
                 pr.referenced_doi, 
                 GROUP_CONCAT(pr.publication_doi, ', ') AS citing_publications
             FROM 
                 publication_references pr
             GROUP BY 
                 pr.referenced_doi) citing ON p.id = citing.referenced_doi
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            a.orcid = ?
        GROUP BY
            p.id, p.title, p.publication_venue, pub.name, pub.crossref, p.type, p.issue, p.volume, p.chapter_number, p.event;
        """
        df = pd.read_sql_query(query, conn, params=(author_id,))
        conn.close()
        return df

    
    def getMostCitedPublication(self):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_venue AS publicationVenues,
            (SELECT GROUP_CONCAT(a2.given_name || ' ' || a2.family_name, ', ')
             FROM authors a2 
             WHERE a2.publication_doi = p.id) AS authors,
            citationCount,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.type AS venueTypes,
            p.issue AS issues,
            p.volume AS volumes,
            p.chapter_number AS chapterNumbers,
            p.event AS events
        FROM 
            (SELECT pr.referenced_doi, COUNT(*) AS citationCount
             FROM publication_references pr
             GROUP BY pr.referenced_doi
             ORDER BY COUNT(*) DESC
             LIMIT 1) AS most_cited
        JOIN 
            publications p ON most_cited.referenced_doi = p.id
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def getMostCitedVenue(self):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
        query = """
        SELECT 
            p.publication_venue AS venue,
            p.type AS venueTypes,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            SUM(citationCount) AS totalCitations
        FROM 
            (SELECT 
                 pr.referenced_doi, 
                 COUNT(*) AS citationCount
             FROM 
                 publication_references pr
             GROUP BY 
                 pr.referenced_doi) AS citation_counts
        JOIN 
            publications p ON citation_counts.referenced_doi = p.id
        JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        GROUP BY 
            p.publication_venue, p.type, pub.name, pub.crossref
        ORDER BY 
            totalCitations DESC
        LIMIT 1;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def getVenuesByPublisherId(self, publisher_id):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_venue AS publicationVenues,
            (SELECT GROUP_CONCAT(a2.given_name || ' ' || a2.family_name, ', ')
             FROM authors a2 
             WHERE a2.publication_doi = p.id) AS authors,
            pr_agg.citing_publications AS citedPublications,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.type AS venueTypes,
            p.issue AS issues,
            p.volume AS volumes,
            p.chapter_number AS chapterNumbers,
            p.event AS events
        FROM 
            publications p
        LEFT JOIN 
            venues v ON p.id = v.publication_doi
        LEFT JOIN 
            (SELECT 
                 pr.referenced_doi, 
                 GROUP_CONCAT(pr.publication_doi, ', ') AS citing_publications
             FROM 
                 publication_references pr
             GROUP BY 
                 pr.referenced_doi) pr_agg ON p.id = pr_agg.referenced_doi
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            pub.id = ?;
        """
        df = pd.read_sql_query(query, conn, params=(publisher_id,))
        conn.close()
        return df

    def getJournalArticlesInVolume(self, issn, volume):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None

        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_year AS publicationYears,
            p.issue AS issues,
            p.volume AS volumes,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.publication_venue AS publicationVenues,
            p.type AS venueTypes
        FROM 
            publications p
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        LEFT JOIN 
            venues v ON p.id = v.publication_doi
        WHERE 
            v.issn = ? AND
            p.volume = ? AND
            p.type = 'journal-article';
        """

        df = pd.read_sql_query(query, conn, params=(issn, volume))
        conn.close()
        return df    
    
    def getJournalArticlesInIssue(self, issn, volume, issue):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_year AS publicationYears,
            p.issue AS issues,
            p.volume AS volumes,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.publication_venue AS publicationVenues,
            p.type AS venueTypes
        FROM 
            publications p
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        LEFT JOIN 
            venues v ON p.id = v.publication_doi
        WHERE 
            v.issn = ? AND
            p.type = 'journal-article' AND
            p.issue = ? AND
            p.volume = ?;
        """
    
        df = pd.read_sql_query(query, conn, params=(issn, volume, issue))
        conn.close()
        return df
    
    def getPublicationInVenue(self, venue_identifier):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_venue AS publicationVenues,
            (SELECT GROUP_CONCAT(a2.given_name || ' ' || a2.family_name, ', ')
             FROM authors a2 
             WHERE a2.publication_doi = p.id) AS authors,
            pr_agg.citing_publications AS citedPublications,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.type AS venueTypes,
            p.issue AS issues,
            p.volume AS volumes,
            p.chapter_number AS chapterNumbers,
            p.event AS events
        FROM 
            publications p
        LEFT JOIN 
            venues v ON p.id = v.publication_doi
        LEFT JOIN 
            (SELECT 
                 pr.referenced_doi, 
                 GROUP_CONCAT(pr.publication_doi, ', ') AS citing_publications
             FROM 
                 publication_references pr
             GROUP BY 
                 pr.referenced_doi) pr_agg ON p.id = pr_agg.referenced_doi
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            v.issn = ? OR v.isbn = ?
        """
    
        df = pd.read_sql_query(query, conn, params=(venue_identifier, venue_identifier))
        conn.close()
        return df

    
    def getJournalArticlesInJournal(self, issn):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_year AS publicationYears,
            p.issue AS issues,
            p.volume AS volumes,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.publication_venue AS publicationVenues,
            p.type AS venueTypes
        FROM 
            publications p
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        LEFT JOIN 
            venues v ON p.id = v.publication_doi
        WHERE 
            v.issn = ? AND
            p.type = 'journal-article';
        """
    
        df = pd.read_sql_query(query, conn, params=(issn,))
        conn.close()
        return df
    
    def getProceedingsByEvent(self, event_name):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_year AS publicationYears,
            p.issue AS issues,
            p.volume AS volumes,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.publication_venue AS publicationVenues,
            p.type AS venueTypes
        FROM 
            publications p
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            p.type = 'proceedings' AND
            LOWER(p.event) LIKE '%' || LOWER(?) || '%';
        """
    
        df = pd.read_sql_query(query, conn, params=('%' + event_name.lower() + '%',))
        conn.close()
        return df
    
    def getPublicationAuthors(self, publication_doi):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        query = """
        SELECT 
            orcid,
            family_name AS familyName,
            given_name AS givenName
        FROM 
            authors
        WHERE 
            publication_doi = ?;
        """
    
        df = pd.read_sql_query(query, conn, params=(publication_doi,))
        conn.close()
        return df
    
    def getPublicationsByAuthorName(self, author_name):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        query = """
        SELECT 
            p.id,
            p.title,
            p.publication_venue AS publicationVenues,
            (SELECT GROUP_CONCAT(a2.given_name || ' ' || a2.family_name, ', ')
             FROM authors a2 
             WHERE a2.publication_doi = p.id) AS authors,
            (SELECT GROUP_CONCAT(pr2.publication_doi, ', ')
             FROM publication_references pr2
             WHERE pr2.referenced_doi = p.id) AS citedPublications,
            pub.name AS publisherNames,
            pub.crossref AS crossrefIDs,
            p.type AS venueTypes,
            p.issue AS issues,
            p.volume AS volumes,
            p.chapter_number AS chapterNumbers,
            p.event AS events
        FROM 
            publications p
        LEFT JOIN 
            authors a ON p.id = a.publication_doi
        LEFT JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            LOWER(a.given_name || ' ' || a.family_name) LIKE '%' || LOWER(?) || '%'
        GROUP BY
            p.id, p.title, p.publication_venue, pub.name, pub.crossref, p.type, p.issue, p.volume, p.chapter_number, p.event;
        """
    
        name_pattern = f'%{author_name.lower()}%'
        df = pd.read_sql_query(query, conn, params=(name_pattern,))
        conn.close()
        return df
    
    
    def getDistinctPublisherOfPublications(self, publication_dois):
        # Use the connection from the associated RelationalDataProcessor
        if self.db_processor and self.db_processor.connection:
            conn = self.db_processor.connection
        else:
            print("Database connection not established.")
            return None
    
        placeholders = ', '.join(['?'] * len(publication_dois))
        query = f"""
        SELECT DISTINCT
            pub.id AS publisherID,
            pub.name AS publisherName
        FROM 
            publications p
        JOIN 
            publishers pub ON p.publisher_crossref = pub.id
        WHERE 
            p.id IN ({placeholders});


        """
    
        df = pd.read_sql_query(query, conn, params=publication_dois)
        conn.close()
        return df

'''
# Set display options to show all columns and rows without truncation
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

# Initialization of the database processor and query processor
db_path = 'aath_to_your_database.db'
db_processor = RelationalProcessor(db_path)  # Create an instance of RelationalProcessor
query_processor = RelationalQueryProcessor()
query_processor.setDbProcessor(db_processor)  


# Example usage with standardized print calls
display(query_processor.getPublicationsPublishedInYear(2020))

display(query_processor.getPublicationsByAuthorId("0000-0001-9857-1511"))

display(query_processor.getMostCitedPublication())

display(query_processor.getMostCitedVenue())

display(query_processor.getVenuesByPublisherId("crossref:78"))

display(query_processor.getJournalArticlesInVolume("issn:2641-3337", "1"))

display(query_processor.getJournalArticlesInIssue('issn:2641-3337', '1', '1'))

display(query_processor.getJournalArticlesInVolume('issn:2164-5515', '17'))

display(query_processor.getJournalArticlesInJournal('issn:2164-5515'))

display(query_processor.getProceedingsByEvent('web'))

display(query_processor.getPublicationAuthors('doi:10.1080/21645515.2021.1910000'))



display(query_processor.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"]))

'''
display(query_processor.getPublicationsByAuthorName('Peroni'))


# In[494]:


class QueryProcessor():
        pass

class GenericQueryProcessor(QueryProcessor):
    def __init__(self):
        self.query_processors = []

    def clean_query_processors(self):
        self.query_processors.clear()
        return True

    def addQueryProcessor(self, query_processor):
        self.query_processors.append(query_processor)
    
    def getPublicationsPublishedInYear(self, year):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getPublicationsPublishedInYear(year)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")
    
        # Combine all results into a single DataFrame
        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()  # Return an empty DataFrame if no results

    def getPublicationsByAuthorId(self, author_id):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getPublicationsByAuthorId(author_id)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame() 

    def getMostCitedPublication(self):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getMostCitedPublication()
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame() 

    def getMostCitedVenue(self):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getMostCitedVenue()
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()  
            
    def getVenuesByPublisherId(self, publisher_id):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getVenuesByPublisherId(publisher_id)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()

    def getPublicationInVenue(self, venue_id):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getPublicationInVenue(venue_id)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame() 

    def getJournalArticlesInIssue(self, journal_id, volume, issue):
        merged_results = []
        for processor in self.query_processors:
            # Directly pass the parameters to the processor's method
            result = processor.getJournalArticlesInIssue(journal_id, volume, issue)
            
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()

    def getJournalArticlesInVolume(self, issn, volume):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getJournalArticlesInVolume(issn, volume)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()

    def getJournalArticlesInJournal(self, issn):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getJournalArticlesInJournal(issn)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()

    def getProceedingsByEvent(self, event_name):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getProceedingsByEvent(event_name)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()

    def getPublicationAuthors(self, publication_id):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getPublicationAuthors(publication_id)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame() 

    def getPublicationsByAuthorName(self, author_name):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getPublicationsByAuthorName(author_name)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame()

    def getDistinctPublisherOfPublications(self, publication_ids):
        merged_results = []
        for processor in self.query_processors:
            result = processor.getDistinctPublisherOfPublications(publication_ids)
            if result is not None and not result.empty:
                merged_results.append(result)
            else:
                print(f"No data or empty result from {type(processor).__name__}")

        if merged_results:
            combined_results = pd.concat(merged_results, ignore_index=True)
            return combined_results
        else:
            return pd.DataFrame() 


# In[488]:


# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "aath_to_your_database.db"
rel_dp = RelationalDataProcessor()
rel_dp.db_processor.setDbPath(rel_path)  
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("/Users/juanpablocasadobissone/Downloads/graph_publications.csv")
grp_dp.uploadData("/Users/juanpablocasadobissone/Downloads/graph_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbProcessor(rel_dp.db_processor)  

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

# Create a generic query processor and add the specific processors
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

# Set display options to show all columns and rows without truncation
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

'''
result = generic.getPublicationsPublishedInYear(2018)
display(result)
'''
'''
result_q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
display(result_q2)
'''
'''
result_q3 = generic.getMostCitedPublication()
display(result_q3)
'''
'''
result_q4 = generic.getMostCitedVenue()
display(result_q4)
'''
'''
result_q4 = generic.getVenuesByPublisherId("crossref:78")
display(result_q4)
'''
'''
result_q5 = generic.getPublicationInVenue("issn:0944-1344")
display(result_q5)
'''
'''
result_q6 = generic.getJournalArticlesInIssue('issn:1061-4036', '50', '4')
display(result_q6)
'''
'''
result_journal_volume = generic.getJournalArticlesInVolume("issn:1061-4036", "50")
display(result_journal_volume)
'''
'''
result_journal = generic.getJournalArticlesInJournal("issn:1061-4036")
display(result_journal)
'''
'''
result_publication_authors = generic.getProceedingsByEvent("web")
display(result_proceedings)
'''
'''
result_authors = generic.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")
display(result_authors)
'''
'''
result_publications_by_author = generic.getPublicationsByAuthorName("peroni")
display(result_publications_by_author)
'''
'''
result_publishers = generic.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])
display(result_publishers)
'''


# In[ ]:





# In[ ]:





# In[ ]:




