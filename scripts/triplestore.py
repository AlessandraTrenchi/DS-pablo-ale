#!/usr/bin/env python
# coding: utf-8

# In[199]:


# Starting with necessary imports
from SPARQLWrapper import SPARQLWrapper, POST, JSON
import pandas as pd
import json
import re
from urllib.parse import quote
from urllib.parse import unquote  


# In[200]:


def decode_percent_encoding(s):
    return unquote(s)

def normalize_nested_json(data):
    """Recursively normalize JSON data."""
    # If the data is a dictionary, iterate through its key-value pairs
    if isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
            # Normalize the key if it starts with "doi:"
            new_key = encode_colon_in_doi(key)
            # Recursively normalize the value
            new_value = normalize_nested_json(value)
            # Store the new key-value pair in the new_data dictionary
            new_data[new_key] = new_value
        return new_data
    # If the data is a list, iterate through its items
    elif isinstance(data, list):
        return [normalize_nested_json(item) for item in data]
    # If the data is a string and starts with "doi:", normalize it
    elif isinstance(data, str) and "doi:" in data:
        decoded_str = decode_percent_encoding(data)
        return encode_colon_in_doi(decoded_str) if "doi:" in decoded_str else decoded_str

    else:
        # No normalization needed for other types like numbers, None, etc.
        return data

def encode_colon_in_doi(uri):
    if "doi:" in uri:
        parts = uri.split("doi:")
        encoded_doi = parts[1].replace(":", "%3A")
        return f"doi:{encoded_doi}"
    return uri

# Paths to the original and new JSON files
input_json_path = "/Users/juanpablocasadobissone/Downloads/graph_other_data copy.json"
output_json_path = "/Users/juanpablocasadobissone/Downloads/graph_other_data_cleaned.json"

# Read the original JSON
with open(input_json_path, 'r') as f:
    original_data = json.load(f)

# Normalize the data
normalized_data = normalize_nested_json(original_data)

# Write the normalized JSON
with open(output_json_path, 'w') as f:
    json.dump(normalized_data, f)


# In[201]:


def decode_percent_encoding(s):
    return unquote(s)

# Define a function to normalize the CSV
def normalize_csv(input_csv_path, output_csv_path):
    # Read the CSV into a DataFrame
    df = pd.read_csv(input_csv_path)
    
    # Fill missing values with a default value
    df.fillna("Unknown", inplace=True)
    
    # Check if 'doi' column exists and normalize it
    if 'doi' in df.columns:
        df['doi'] = df['doi'].apply(encode_colon_in_doi)
    
    # Check for other columns that might need normalization and handle them here
    # For example:
    if 'title' in df.columns:
        df['title'] = df['title'].apply(decode_percent_encoding)
    if 'publication_venue' in df.columns:
        df['publication_venue'] = df['publication_venue'].apply(lambda x: quote(x))
    if 'publisher' in df.columns:
        df['publisher'] = df['publisher'].apply(lambda x: quote(x))
        
    # Save the normalized DataFrame to a new CSV file
    df.to_csv(output_csv_path, index=False)

# Paths to the original and new CSV files
input_csv_path = "/Users/juanpablocasadobissone/Downloads/graph_publications.csv"
output_csv_path = "/Users/juanpablocasadobissone/Downloads/graph_publications_cleaned.csv"

# Run the normalization function
normalize_csv(input_csv_path, output_csv_path)


# In[202]:


class TriplestoreDataProcessor:
    def __init__(self):
        self.endpointUrl = "http://localhost:9999/blazegraph/sparql"
        self.base_uri = "http://schema.org/"
        self.custom_base_uri = "https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/"
        
        # Define a mapping between CSV column names and their corresponding URIs
        self.column_to_uri_mapping = {
            'title': self.base_uri + 'headline',
            'publication_year': self.base_uri + 'datePublished',
            'journal': self.base_uri + 'isPartOf',
            'doi': self.base_uri + 'identifier',
            'name': self.base_uri + 'name',
            'affiliation': self.base_uri + 'affiliation',
            'identifier': self.base_uri + 'identifier',
            'type': self.base_uri + 'additionalType',
            'location': self.base_uri + 'address',
            'venue_identifier': self.base_uri + 'identifier',
            'venue_name': self.base_uri + 'name',
            'venue_type': self.base_uri + 'additionalType',
            'publisher_identifier': self.base_uri + 'identifier',
            'publisher_name': self.base_uri + 'name',
            'publisher_location': self.base_uri + 'address',
            }

    def clear_all_data(self):
        sparql = SPARQLWrapper(self.endpointUrl)
        clear_query = "DELETE WHERE { ?s ?p ?o }"
        sparql.setQuery(clear_query)
        sparql.method = 'POST'
        sparql.query()


    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, url):
        self.endpointUrl = url

    def uploadData(self, filepath):
        # Read data
        if filepath.endswith(".csv"):
            data = pd.read_csv(filepath)
            triples = self.convert_data_to_triples_corrected(data)
        elif filepath.endswith(".json"):
            with open(filepath, 'r') as f:
                data = json.load(f)
            triples = self.convert_data_to_triples_corrected(data)
        else:
            raise ValueError("Unsupported file format")

        print(f"Uploading data from file: {filepath}")
        print(f"Number of triples: {len(triples)}")

        # Upload triples to the triplestore
        self.upload_triples_to_store(triples)

    def convert_data_to_triples_corrected(self, data):
        # Handling CSV data
        triples = []
        base_uri = "http://schema.org/"
        custom_base_uri = "https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/"

        if isinstance(data, pd.DataFrame):  # For CSV data
            for _, row in data.iterrows():
               
                # Construct the URI for the publication
                publication_uri = f"<{base_uri}publication/{row['id']}>"
                
                # Generate the triples
                triples.append(f"{publication_uri} a <{base_uri}ScholarlyArticle> .")
                triples.append(f"{publication_uri} <{base_uri}headline> \"{row['title']}\" .")
                triples.append(f"{publication_uri} <{base_uri}datePublished> \"{row['publication_year']}\" .")
                if not pd.isna(row['issue']):
                    triples.append(f"{publication_uri} <{base_uri}issueNumber> \"{row['issue']}\" .")
                if not pd.isna(row['volume']):
                    triples.append(f"{publication_uri} <{base_uri}volumeNumber> \"{row['volume']}\" .")
                if not pd.isna(row['chapter']):
                    triples.append(f"{publication_uri} <{base_uri}chapter> \"{row['chapter']}\" .")
                if not pd.isna(row['publication_venue']):
                    triples.append(f"{publication_uri} <{base_uri}isPartOf> <{base_uri}venue/{row['publication_venue']}> .")
                if not pd.isna(row['venue_type']):
                    triples.append(f"{publication_uri} <{base_uri}type> <{base_uri}{row['venue_type']}> .")
                if not pd.isna(row['publisher']):
                    triples.append(f"{publication_uri} <{base_uri}publisher> <{base_uri}publisher/{row['publisher']}> .")
                if not pd.isna(row['event']):
                    triples.append(f"{publication_uri} <{base_uri}about> <{base_uri}event/{row['event']}> .")
            return triples
            
        # Handling JSON data
        else:
            for pub_id, authors in data['authors'].items():
                for author in authors:
                    author_uri = f"<{self.base_uri}author/{author['orcid']}>"
            
                    # This is the line that was missing, specifying the type as Author
                    triples.append(f"{author_uri} a <{self.base_uri}Author> .")  # <-- ADDED BACK
            
                    # Add these lines to create triples for 'family', 'given', and 'orcid'
                    triples.append(f"{author_uri} <{self.base_uri}familyName> \"{author['family']}\" .")
                    triples.append(f"{author_uri} <{self.base_uri}givenName> \"{author['given']}\" .")
                    triples.append(f"{author_uri} <{self.base_uri}orcid> \"{author['orcid']}\" .")
            
                    # Moved this line inside the author loop
                    triples.append(f"<{self.base_uri}publication/{pub_id}> <{self.base_uri}author> {author_uri} .")  # <-- MOVED HERE
        
                    
                            
            # Convert venues_id data to triples
            for pub_id, venue_ids in data['venues_id'].items():
                for venue_id in venue_ids:
                    triples.append(f"<{self.base_uri}{pub_id}> <{self.base_uri}venue> <{self.base_uri}venue/{venue_id}> .")
                    
            # Convert publishers data to triples
            for publisher_id, publisher_details in data['publishers'].items():
                publisher_uri = f"<{self.base_uri}publisher/{publisher_id}>"
                triples.append(f"{publisher_uri} <{self.base_uri}type> <{self.base_uri}Publisher> .")
                triples.append(f"{publisher_uri} <{self.base_uri}name> \"{publisher_details['name']}\" .")
                
        # Debugging: Print some triples
        print("Debugging: Some generated triples:")
        print(triples[:10])  # Print first 10 triples
        
        return triples  # Don't forget to return the triples
    def upload_triples_to_store(self, triples):
        print("Inside upload_triples_to_store method")
        
        triples_str = "\n".join(triples)
        sparql_query = f'''
        INSERT DATA {{
            {triples_str}
        }}
        '''
        print(f"Executing SPARQL query:\n{sparql_query}")

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setMethod(POST)
        sparql.setQuery(sparql_query)

        error_file_path = "/Users/juanpablocasadobissone/Downloads/triples.txt"

        # Save the triples to a file
        with open("/Users/juanpablocasadobissone/Downloads/triples_data.txt", "w") as file:
            for triple in triples:
                file.write(triple + "\n")

        try:
            sparql.query()
            print("Triples successfully uploaded to the triplestore.")
        except Exception as e:
            with open(error_file_path, 'w') as error_file:
                error_file.write(str(e))
            print(f"Error while uploading triples. Details saved in: {error_file_path}")
            return  # Terminate the function here
    
    def getPublicationsPublishedInYear(self, year):
        sparql_query = f'''
        SELECT ?publication ?title WHERE {{
            ?publication a <http://schema.org/ScholarlyArticle> ;
                        <http://schema.org/datePublished> "{year}" ;
                        <http://schema.org/headline> ?title .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']
    
    
    def getPublicationsByAuthorId(self, author_id):
        sparql_query = f'''
        SELECT ?publication ?title WHERE {{
            ?publication <{self.base_uri}type> <{self.base_uri}ScholarlyArticle> ;
                        <{self.base_uri}author> <{self.base_uri}author/{author_id}> ;
                        <{self.base_uri}headline> ?title .
        }}
        '''
        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results['results']['bindings']


    def getMostCitedPublication(self):
        sparql_query = f'''
        SELECT ?publication ?title (COUNT(?citingPublication) AS ?citationCount) WHERE {{
            ?citingPublication <http://schema.org/citation> ?publication .
            ?publication <http://schema.org/headline> ?title .
        }}
        GROUP BY ?publication ?title
        ORDER BY DESC(?citationCount)
        LIMIT 1
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getMostCitedVenue(self):
        sparql_query = f'''
        SELECT ?venue (COUNT(?citingPublication) AS ?citationCount) WHERE {{
            ?publication <http://schema.org/isPartOf> ?venue .
            ?citingPublication <http://schema.org/citation> ?publication .
        }}
        GROUP BY ?venue
        ORDER BY DESC(?citationCount)
        LIMIT 1
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getVenuesByPublisherId(self, publisher_id):
        sparql_query = f'''
        SELECT ?venue ?name WHERE {{
            ?venue <http://schema.org/type> <http://schema.org/Periodical> ;
                   <http://schema.org/publisher> <{publisher_id}> ;
                   <http://schema.org/name> ?name .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getPublicationInVenue(self, venue_id):
        sparql_query = f'''
        SELECT ?publication ?title WHERE {{
            ?publication <http://schema.org/type> <http://schema.org/ScholarlyArticle> ;
                          <http://schema.org/isPartOf> <{venue_id}> ;
                          <http://schema.org/headline> ?title .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getJournalArticlesInIssue(self, journal_id, volume_number, issue_number):
        sparql_query = f'''
        SELECT ?article ?title WHERE {{
            ?article <http://schema.org/type> <http://schema.org/ScholarlyArticle> ;
                     <http://schema.org/isPartOf> ?issue .
            ?issue <http://schema.org/type> <http://schema.org/PublicationIssue> ;
                   <http://schema.org/issueNumber> "{issue_number}" ;
                   <http://schema.org/isPartOf> ?volume .
            ?volume <http://schema.org/type> <http://schema.org/PublicationVolume> ;
                    <http://schema.org/volumeNumber> "{volume_number}" ;
                    <http://schema.org/isPartOf> <{journal_id}> ;
                    <http://schema.org/headline> ?title .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getJournalArticlesInVolume(self, journal_id, volume_number):
        sparql_query = f'''
        SELECT ?article ?title WHERE {{
            ?article <http://schema.org/type> <http://schema.org/ScholarlyArticle> ;
                     <http://schema.org/isPartOf> ?volume .
            ?volume <http://schema.org/type> <http://schema.org/PublicationVolume> ;
                    <http://schema.org/volumeNumber> "{volume_number}" ;
                    <http://schema.org/isPartOf> <{journal_id}> ;
                    <http://schema.org/headline> ?title .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getJournalArticlesInJournal(self, journal_id):
        sparql_query = f'''
        SELECT ?article ?title WHERE {{
            ?article <http://schema.org/type> <http://schema.org/ScholarlyArticle> ;
                     <http://schema.org/isPartOf> ?volume .
            ?volume <http://schema.org/type> <http://schema.org/PublicationVolume> ;
                    <http://schema.org/isPartOf> <{journal_id}> ;
                    <http://schema.org/headline> ?title .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getProceedingsByEvent(self, event_name):
        event_name_lower = event_name.lower()
        sparql_query = f'''
        SELECT ?proceeding ?title WHERE {{
            ?proceeding <http://schema.org/type> <http://schema.org/ScholarlyArticle> ;
                        <http://schema.org/about> ?event ;
                        <http://schema.org/headline> ?title .
            ?event <http://schema.org/name> ?event_name .
            FILTER(STRSTARTS(LCASE(?event_name), "{event_name_lower}"))
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getPublicationAuthors(self, publication_id):
        sparql_query = f'''
        SELECT ?author ?name WHERE {{
            <{publication_id}> <http://schema.org/author> ?author .
            ?author <http://schema.org/name> ?name .
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getPublicationsByAuthorName(self, author_name):
        author_name_lower = author_name.lower()
        sparql_query = f'''
        SELECT ?publication ?title WHERE {{
            ?publication <http://schema.org/author> ?author .
            ?author <http://schema.org/name> ?name .
            ?publication <http://schema.org/headline> ?title .
            FILTER(CONTAINS(LCASE(?name), "{author_name_lower}"))
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getDistinctPublisherOfPublications(self, publication_ids):
        filter_condition = " || ".join([f"?publication = <{pub_id}>" for pub_id in publication_ids])
        sparql_query = f'''
        SELECT DISTINCT ?publisher ?name WHERE {{
            ?publication <http://schema.org/isPartOf> ?venue .
            ?venue <http://schema.org/publisher> ?publisher .
            ?publisher <http://schema.org/name> ?name .
            FILTER({filter_condition})
        }}
        '''

        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        return results['results']['bindings']

    def getPublicationsByAuthorOrcid(self, orcid):
        sparql_query = f'''
        SELECT ?publication ?title WHERE {{
            ?publication <{self.base_uri}author> <{self.base_uri}author/{orcid}> ;
                        <{self.base_uri}headline> ?title .
        }}
        '''
        sparql = SPARQLWrapper(self.endpointUrl)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results['results']['bindings']


# In[204]:


# To use this script:
processor = TriplestoreDataProcessor()
processor.clear_all_data()  # This will clear all data

# For JSON data
json_path = "/Users/juanpablocasadobissone/Downloads/graph_other_data_cleaned.json"
processor.uploadData(json_path)

# For CSV data
csv_path = "/Users/juanpablocasadobissone/Downloads/graph_publications_cleaned.csv"
processor.uploadData(csv_path)


# In[205]:


processor = TriplestoreDataProcessor()
processor.setEndpointUrl("http://localhost:9999/blazegraph/sparql")

# Test getPublicationsByAuthorOrcid
result = processor.getPublicationsByAuthorOrcid("0000-0002-3938-2064")
print("Publications by Author ORCID: ", result)


# In[ ]:




