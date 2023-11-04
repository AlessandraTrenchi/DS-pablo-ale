#!/usr/bin/env python
# coding: utf-8

# In[19]:


import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, JSON
import sqlite3
import logging
from urllib.parse import quote
import urllib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Main Processor classes

class RelationalProcessor:
    def __init__(self, db_path=None):
        self.dbPath = db_path

    def getDbPath(self):
        return self.dbPath

    def setDbPath(self, path):
        self.dbPath = path

'''
class TriplestoreProcessor:
    def __init__(self, endpoint_url=None):
        self.endpointUrl = endpoint_url

    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, url):
        self.endpointUrl = url
'''
# Data Processor classes

class RelationalDataProcessor(RelationalProcessor):
    def __init__(self, db_path=None):
        super().__init__(db_path)

    def uploadData(self, file_path):


'''
class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self, endpoint_url="http://localhost:9999/blazegraph/sparql"):
        super().__init__(endpoint_url)
        self.sparql = SPARQLWrapper(self.endpointUrl)  # Use the same attribute name
        self.base_uri = "http://schema.org/"
        self.custom_base_uri = "https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/"

    # Define a mapping between CSV column names and their corresponding URIs
    PREDICATE_MAP = {
        'title': 'http://schema.org/headline',
        'publication_year': 'http://schema.org/datePublished',
        'author_id': 'https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/hasAuthorID',
        'journal': 'http://schema.org/isPartOf',
        'doi': 'http://schema.org/identifier',
        'citations': 'https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/hasCitations',
        'name': 'http://schema.org/name',
        'id': 'https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/hasAuthorID',
        'affiliation': 'http://schema.org/affiliation',
        'orcid': 'https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/hasORCID',
        'identifier': 'http://schema.org/identifier',
        'type': 'http://schema.org/additionalType',
        'location': 'http://schema.org/address',
        'venue_identifier': 'http://schema.org/identifier',
        'venue_name': 'http://schema.org/name',
        'venue_type': 'http://schema.org/additionalType',
        'publisher_identifier': 'http://schema.org/identifier',
        'publisher_name': 'http://schema.org/name',
        'publisher_location': 'http://schema.org/address'
}

    def upload_triples_to_blazegraph(self, triples):
        # Convert the list of triples into a SPARQL INSERT DATA command
        sparql_update_query = self._format_sparql_insert_query(triples)
        
        try:
            self.sparql.setMethod(POST)
            self.sparql.setQuery(sparql_update_query)
            self.sparql.setReturnFormat(JSON)
            
            response = self.sparql.query()
            logging.info(f"Blazegraph response: {response.response.read().decode('utf-8')}")
    
        except Exception as e:
            logging.error(f"An error occurred while uploading triples: {e}")
            # Optionally re-raise the exception if you want to handle it further up the call stack
            # raise

    def _format_sparql_insert_query(self, triples):
        # Helper method to format the SPARQL query for inserting data
        triples_as_string = ' .\n'.join([' '.join(triple) for triple in triples])
        return f"""
        PREFIX schema: <http://schema.org/>
        INSERT DATA {{
            {triples_as_string} .
        }}
        """


    def format_object(self, value, column):
        # Format the object based on UML data type definitions
        if isinstance(value, int):
            # Integer literal
            return '"' + str(value) + '"^^<http://www.w3.org/2001/XMLSchema#integer>'
        elif isinstance(value, float):
            # Float literal
            return '"' + str(value) + '"^^<http://www.w3.org/2001/XMLSchema#float>'
        elif isinstance(value, bool):
            # Boolean literal
            return '"' + str(value).lower() + '"^^<http://www.w3.org/2001/XMLSchema#boolean>'
        elif isinstance(value, str):
            if column == 'publicationYear':  # assuming the publication year is an integer
                return '"' + value + '"^^<http://www.w3.org/2001/XMLSchema#integer>'
            # String literal, escape quotes in the string itself and wrap with quotes
            escaped_value = value.replace('"', '\\"')
            return '"' + escaped_value + '"^^<http://www.w3.org/2001/XMLSchema#string>'
        else:
            # Default to string if unsure
            return '"' + str(value) + '"^^<http://www.w3.org/2001/XMLSchema#string>'
    

    def convert_json_to_triples(self, json_data):
        triples = []
        
        if 'authors' in json_data:
            for doi, authors_list in json_data['authors'].items():
                for author in authors_list:
                    author_uri = f"{self.custom_base_uri}Person/{urllib.parse.quote(author['orcid'])}"
                    publication_uri = f"{self.custom_base_uri}Publication/{urllib.parse.quote(doi)}"
                    predicate = f"<{self.base_uri}authorOf>"
                    triples.append((f"<{author_uri}>", predicate, f"<{publication_uri}>"))
        
        # Handle 'references' part of JSON
        if 'references' in json_data:
            for doi, references_list in json_data['references'].items():
                for reference in references_list:
                    reference_uri = f"{self.custom_base_uri}Publication/{urllib.parse.quote(reference)}"
                    publication_uri = f"{self.custom_base_uri}Publication/{urllib.parse.quote(doi)}"
                    predicate = f"<{self.base_uri}cites>"
                    triples.append((f"<{publication_uri}>", predicate, f"<{reference_uri}>"))
        
        # Handle 'venues_id' part of JSON
        if 'venues_id' in json_data:
            for doi, venue_ids in json_data['venues_id'].items():
                for venue_id in venue_ids:
                    venue_uri = f"{self.custom_base_uri}Venue/{urllib.parse.quote(venue_id)}"
                    publication_uri = f"{self.custom_base_uri}Publication/{urllib.parse.quote(doi)}"
                    predicate = f"<{self.base_uri}publishedIn>"
                    triples.append((f"<{publication_uri}>", predicate, f"<{venue_uri}>"))
        
        # Handle 'publishers' part of JSON
        if 'publishers' in json_data:
            for doi, venue_details in json_data.get('venues_id', {}).items():
                # Assume the first venue ID in the list is the primary one for mapping to the publisher
                venue_id = venue_details[0] if venue_details else None
                # Using the venue ID, get the associated publisher ID (if available)
                publisher_id = json_data.get('venue_to_publisher', {}).get(venue_id)
                if publisher_id:
                    publisher_details = json_data['publishers'].get(publisher_id)
                    if publisher_details:
                        publisher_uri = f"{self.custom_base_uri}Organization/{urllib.parse.quote(publisher_details['id'])}"
                        publication_uri = f"{self.custom_base_uri}Publication/{urllib.parse.quote(doi)}"
                        predicate = f"<{self.base_uri}publisher>"
                        triples.append((f"<{publication_uri}>", predicate, f"<{publisher_uri}>"))
        
        return triples


    
    def convert_dataframe_to_triples(self, data_frame, entity_type):
        triples = []
        for index, row in data_frame.iterrows():
            # Generate a URI for the entity
            entity_uri = f"{self.custom_base_uri}{entity_type}/{urllib.parse.quote(str(row['id']))}"
            entity_subject = f"<{entity_uri}>"

            # Add the rdf:type triple
            rdf_type_uri = f"{self.custom_base_uri}{entity_type}"  # entity_type could be 'JournalArticle', 'BookChapter', etc.
            rdf_type_predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            rdf_type_object = f"<{rdf_type_uri}>"
            triples.append((entity_subject, rdf_type_predicate, rdf_type_object))

        # Convert attributes to triples
            for column, value in row.items():
                if column in self.PREDICATE_MAP and pd.notna(value):
                    predicate_uri = self.PREDICATE_MAP[column]
                    predicate = f"<{predicate_uri}>"
                    object = self.format_object(value, column)
                    triples.append((entity_subject, predicate, object))
      
            # Handle the properties specific to specialized publication types
            if entity_type == 'JournalArticle':
                if 'issue' in row and pd.notna(row['issue']):
                    issue_predicate = f"<{self.base_uri}issue>"
                    issue_object = f"\"{row['issue']}\""
                    triples.append((entity_subject, issue_predicate, issue_object))
                if 'volume' in row and pd.notna(row['volume']):
                    volume_predicate = f"<{self.base_uri}volume>"
                    volume_object = f"\"{row['volume']}\""
                    triples.append((entity_subject, volume_predicate, volume_object))
            
            elif entity_type == 'BookChapter':
                if 'chapterNumber' in row and pd.notna(row['chapterNumber']):
                    chapter_predicate = f"<{self.base_uri}chapterNumber>"
                    chapter_object = f"\"{row['chapterNumber']}\"^^<http://www.w3.org/2001/XMLSchema#integer>"
                    triples.append((entity_subject, chapter_predicate, chapter_object))
            
            elif entity_type == 'ProceedingsPaper':
                if 'event' in row and pd.notna(row['event']):
                    event_predicate = f"<{self.base_uri}event>"
                    event_object = f"\"{row['event']}\""
                    triples.append((entity_subject, event_predicate, event_object))

            # Handle complex relationships
            if 'cites' in row and pd.notna(row['cites']):
                # Assuming 'cites' is a list of publication IDs
                for cited_id in row['cites']:
                    cited_uri = f"{self.custom_base_uri}Publication/{urllib.parse.quote(cited_id)}"
                    predicate = f"<{self.base_uri}cites>"
                    object = f"<{cited_uri}>"
                    triples.append((entity_subject, predicate, object))

            if 'author' in row and pd.notna(row['author']):
                # Assuming 'author' is a list of author IDs
                for author_id in row['author']:
                    author_uri = f"{self.custom_base_uri}Person/{urllib.parse.quote(author_id)}"
                    predicate = f"<{self.base_uri}author>"
                    object = f"<{author_uri}>"
                    triples.append((entity_subject, predicate, object))

            if 'publicationVenue' in row and pd.notna(row['publicationVenue']):
                venue_uri = f"{self.custom_base_uri}Venue/{urllib.parse.quote(row['publicationVenue'])}"
                predicate = f"<{self.base_uri}publicationVenue>"
                object = f"<{venue_uri}>"
                triples.append((entity_subject, predicate, object))

            if 'publisher' in row and pd.notna(row['publisher']):
                publisher_uri = f"{self.custom_base_uri}Organization/{urllib.parse.quote(row['publisher'])}"
                predicate = f"<{self.base_uri}publisher>"
                object = f"<{publisher_uri}>"
                triples.append((entity_subject, predicate, object))


        return triples
    

    def uploadData(self, file_path):
        try:
            file_extension = file_path.split('.')[-1].lower()
            
            if file_extension == 'csv':
                logging.info("Uploading CSV data to the triplestore...")
                data_frame = pd.read_csv(file_path)
    
                # Determine the entity_type based on the columns present in the dataframe
                if {'issue', 'volume'}.issubset(data_frame.columns):
                    entity_type = 'JournalArticle'
                elif 'chapterNumber' in data_frame.columns:
                    entity_type = 'BookChapter'
                elif 'event' in data_frame.columns:
                    entity_type = 'ProceedingsPaper'
                elif 'familyName' in data_frame.columns and 'givenName' in data_frame.columns:
                    entity_type = 'Person'
                elif 'title' in data_frame.columns and 'publisher' in data_frame.columns:  # This might indicate a Venue entity
                    entity_type = 'Venue'
                else:
                    # If the CSV doesn't match any known entity type, raise an error
                    raise ValueError("Could not determine the entity type from CSV columns")
    
                triples = self.convert_dataframe_to_triples(data_frame, entity_type)
                self.upload_triples_to_blazegraph(triples)
    
        
            elif file_extension == 'json':
                logging.info("Uploading JSON data to the triplestore...")
                with open(file_path, 'r') as json_file:
                    json_data = json.load(json_file)
                    triples = self.convert_json_to_triples(json_data)
                    self.upload_triples_to_blazegraph(triples)
        
            else:
                logging.warning(f"Unsupported file type: {file_extension}")
        except Exception as e:
            logging.error(f"An error occurred while uploading data from file {file_path}: {e}")
            raise

class GenericQueryProcessor:
    def __init__(self):
        self.processors = []

    def addQueryProcessor(self, processor):
        self.processors.append(processor)

    def getPublicationsPublishedInYear(self, year: int):
        results = []
        for processor in self.processors:
            results.extend(processor.getPublicationsPublishedInYear(year))
        return results

'''
# Query Processor classes

class QueryProcessor:
    # This class should define any common behaviors or be an abstract base class.
    pass

class RelationalQueryProcessor(QueryProcessor):
    def __init__(self, db_path=None):
        self.dbPath = db_path
        # You will need a connection to the DB here.
        # self.db_connection = some_db_connection_method(self.dbPath)

    def getPublicationsPublishedInYear(self, year: int):
        # Implement the SQL query logic here
        pass
        
    def getPublicationsByAuthorId(self, author_id: str):
        with sqlite3.connect(self.dbPath) as conn:
            query = "SELECT * FROM publications WHERE author_id = ?"
            return pd.read_sql_query(query, conn, params=[author_id])

    # and others queries definitions

'''
class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self, endpoint_url):
        super().__init__(endpoint_url)  # Make sure to call the superclass initializer if needed
        self.endpointUrl = endpoint_url
        self.sparql = SPARQLWrapper(self.endpointUrl)

    def getPublicationsPublishedInYear(self, year: int):
        query = f"""
        SELECT ?publication ?title WHERE {{
            ?publication <https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/datePublished> ?date .
            ?publication <https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/name> ?title .
            FILTER (YEAR(?date) = {year})
        }}
        """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
    
        # Convert the query results into a DataFrame
        publications = []
        for result in results["results"]["bindings"]:
            publication = result["publication"]["value"]
            title = result["title"]["value"]
            publications.append((publication, title))
    
        df = pd.DataFrame(publications, columns=["Publication", "Title"])
        return df

# Assuming other classes like RelationalQueryProcessor and TriplestoreQueryProcessor will be implemented similarly.






#DEBUG:
# Import additional necessary modules
import pandas as pd
import json
from urllib.parse import quote
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, JSON

# Then, assuming you've defined the TriplestoreDataProcessor class in a previous cell, you can instantiate it
processor = TriplestoreDataProcessor()

# Now let's try to upload a JSON file
json_file_path = '/Users/juanpablocasadobissone/Downloads/graph_other_data(2).json'  # replace with the path to your JSON file
processor.uploadData(json_file_path)

# Now let's try to upload a CSV file
csv_file_path = '/Users/juanpablocasadobissone/Downloads/graph_publications(2).csv'  # replace with the path to your CSV file
processor.uploadData(csv_file_path)
'''


# In[ ]:





# In[ ]:




