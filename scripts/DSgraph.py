#!/usr/bin/env python
# coding: utf-8

# In[12]:


# Import necessary libraries
import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, JSON
from py2neo import Graph

def format_doi_as_uri(doi):
    return "http://dx.doi.org/" + doi.replace(":", "_")

# Define a namespace for rdf:type for better clarity in our code
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


predicate_map = {
    # IdentifiableEntity
    'id': 'http://schema.org/identifier',

    # Publication
    'title': 'http://schema.org/name',
    'type': RDF_TYPE,
    'publication_year': 'http://schema.org/datePublished',
    'publication_venue': 'http://schema.org/isPartOf',
    'authors': 'http://schema.org/author',
    'references': 'http://schema.org/citation',

    # JournalArticle
    'issue': 'http://schema.org/issueNumber',
    'volume': 'http://schema.org/volumeNumber',

    # BookChapter
    'chapter': 'http://schema.org/chapter',

    # ProceedingsPaper
    'event': 'http://schema.org/event',

    # Venue
    'venue_type': 'http://schema.org/additionalType',
    'publisher': 'http://schema.org/publisher',

    # Organization (Publisher)
    # The 'name' attribute for Organization (publisher) can use the same 'http://schema.org/name' IRI as used for title, but it might need a different key in the map if used differently in the code.
    
    # Person
    'family': 'http://schema.org/familyName',
    'given': 'http://schema.org/givenName',
    'orcid': 'http://schema.org/sameAs'  # assuming ORCID is an identifier for a person
}

type_map = {
    'journal_article': 'http://schema.org/ScholarlyArticle',
    'book_chapter': 'http://schema.org/Chapter',
}


# GraphDataProcessor Class
class GraphDataProcessor:
    def __init__(self):
        self.endpointUrl = "http://localhost:9999/blazegraph/sparql"
        self.base_uri = "http://schema.org/"
        self.custom_base_uri = "https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/"

    def setEndpointUrl(self, new_url):
        self.endpointUrl = new_url
        self.sparql = SPARQLWrapper(self.endpointUrl)
    
    def getEndpointUrl(self):
        return self.endpointUrl
    
    def uploadData(self, csv_file, json_file):
        # Step 1: Read and Clean Data
        df_csv = pd.read_csv(csv_file)
        
        # Initialize new columns with data type 'object'
        df_csv['authors'] = pd.Series(dtype='object')
        df_csv['family'] = pd.Series(dtype='object')
        df_csv['given'] = pd.Series(dtype='object')
        df_csv['orcid'] = pd.Series(dtype='object')
        df_csv['venues_id'] = pd.Series(dtype='object')
        df_csv['references'] = pd.Series(dtype='object')
        
        with open(json_file, 'r') as f:
            data_json = json.load(f)
        
        # Step 2: Merge Data
        for doi, authors in data_json['authors'].items():
            author_str = ', '.join([f"{a['given']} {a['family']} ({a['orcid']})" for a in authors])
            family_str = ', '.join([a['family'] for a in authors])
            given_str = ', '.join([a['given'] for a in authors])
            orcid_str = ', '.join([a['orcid'] for a in authors])
            
            df_csv.loc[df_csv['id'] == doi, 'authors'] = author_str
            df_csv.loc[df_csv['id'] == doi, 'family'] = family_str
            df_csv.loc[df_csv['id'] == doi, 'given'] = given_str
            df_csv.loc[df_csv['id'] == doi, 'orcid'] = orcid_str
        
        # Step 3: Normalize Data
        for doi, venues in data_json['venues_id'].items():
            df_csv.loc[df_csv['id'] == doi, 'venues_id'] = ', '.join(venues)
        
        for doi, refs in data_json['references'].items():
            df_csv.loc[df_csv['id'] == doi, 'references'] = ', '.join(refs)
        
        # Store the merged DataFrame
        self.merged_df = df_csv
        
        # Save the merged DataFrame to a local CSV file
        self.merged_df.to_csv('/Users/juanpablocasadobissone/Downloads/data/graph_master_table.csv', index=False)

        # Generate and return triples
        return self.generateTriples()  # Return triples
        
    def generateTriples(self):
        """
        Generate triples from the merged DataFrame.
        
        Triples structure:
            Subject: ID column formatted as a URI
            Predicate: Mapped from predicate_map or custom_base_uri + column name
            Object: The value in the respective column
        """
        # Check if merged DataFrame exists
        if self.merged_df is None:
            print("Error: No merged DataFrame found.")
            return None

        # Initialize list to store triples
        triples = []

        for index, row in self.merged_df.iterrows():
            subject = format_doi_as_uri(row['id'].replace(":", "_"))
            for col in self.merged_df.columns:
                predicate = predicate_map.get(col, self.custom_base_uri + col)
                object_ = row[col]
                if pd.notna(object_):  # Check if object is not NaN
                    # Decide the formatting based on the column type
                    if col in ["venues_id", "id", "type"]:  # We assume venues_id, id, and type are URIs
                        triples.append((subject, predicate, f"<{object_}>"))
                    else:
                        triples.append((subject, predicate, f"\"{object_}\""))
                        
        print(f"Generated {len(triples)} triples.")
        return triples
        



# GraphDatabase Class
class GraphDatabase:
    def __init__(self, blazegraph_endpoint, user, password):
        self.endpointUrl = blazegraph_endpoint 
        self.sparql = SPARQLWrapper(blazegraph_endpoint)
        self.sparql.setHTTPAuth(DIGEST)
        self.sparql.setCredentials(user, password)
        self.sparql.setMethod(POST)

    def getEndpointUrl(self):
        return self.endpointUrl 

    def upload_triples_to_blazegraph(self, triples):
        # Create the SPARQL update query for inserting triples
        print("Uploading triples to Blazegraph...")
        insert_query = "INSERT DATA { GRAPH <https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/> { "
    
        for s, p, o in triples:
            if o.startswith("http:") or o.startswith("\""):  # Check if object is a URI or literal directly
                insert_query += f"<{s}> <{p}> {o} . "
            else:
                insert_query += f"<{s}> <{p}> \"{o}\" . "

        insert_query += " } }"
    
        # Execute the SPARQL update query
        self.sparql.setQuery(insert_query)
        self.sparql.query()
        print("Triples uploaded successfully.")

    def clear_blazegraph(self):
        print("Clearing all data from Blazegraph...")
        clear_query = "CLEAR GRAPH <https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/>"
        self.sparql.setQuery(clear_query)
        self.sparql.query()
        print("Blazegraph cleared successfully.")

    def create_relationship(self, start_node, end_node, relationship_type):
        print("Creating relationship in Blazegraph...")
        
        # Create SPARQL Update query for adding the relationship
        insert_query = f"""
        INSERT DATA {{
            GRAPH <https://github.com/AlessandraTrenchi/DS-pablo-ale/relationaldb/> {{
                <{start_node}> <{relationship_type}> <{end_node}> .
            }}
        }}
        """
        
        # Execute the SPARQL update query
        self.sparql.setQuery(insert_query)
        self.sparql.query()
        print(f"Relationship between {start_node} and {end_node} of type {relationship_type} created successfully.")


# GraphQueryProcessor Class
class GraphQueryProcessor:
    def __init__(self, blazegraph_db):
        self.blazegraph_db = blazegraph_db.getEndpointUrl() 
        self.sparql = SPARQLWrapper(self.blazegraph_db)
        self.sparql.setReturnFormat(JSON) 

    def execute_query(self, query):
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        return results

    def getPublicationsPublishedInYear(self, year):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?publication WHERE {{
            ?publication schema:datePublished "{year}" .
        }}
        """
        results = self.execute_query(query)
        publications = [result["publication"]["value"] for result in results["results"]["bindings"]]
        return publications


    def getMostCitedPublication(self):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?publication (COUNT(?reference) AS ?citationCount) WHERE {{
            ?publication schema:citation ?reference .
        }} GROUP BY ?publication ORDER BY DESC(?citationCount) LIMIT 1
        """
        results = self.execute_query(query)
        if results["results"]["bindings"]:
            return results["results"]["bindings"][0]["publication"]["value"]
        else:
            return None
    
    def getMostCitedVenue(self):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?venue (COUNT(?reference) AS ?citationCount) WHERE {{
            ?publication schema:isPartOf ?venue .
            ?publication schema:citation ?reference .
        }} GROUP BY ?venue ORDER BY DESC(?citationCount) LIMIT 1
        """
        results = self.execute_query(query)
        if results["results"]["bindings"]:
            return results["results"]["bindings"][0]["venue"]["value"]
        else:
            return None

    def getVenuesByPublisherId(self, publisher_id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?venue WHERE {{
            ?venue schema:publisher "{publisher_id}" .
        }}
        """
        return query

    
    def getPublicationInVenue(self, venue_id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?publication ?title WHERE {{
            ?publication schema:isPartOf <{venue_id}> .
            ?publication schema:name ?title .
        }}
        """
        results = self.execute_query(query)
        publications = [{"publication": result["publication"]["value"], "title": result["title"]["value"]} for result in results["results"]["bindings"]]
        return pd.DataFrame(publications) 

    
    def getJournalArticlesInIssue(issue, volume, journal_id):
        query = f"""
        SELECT ?article WHERE {{
            ?article :type "journal-article" .
            ?article :issue "{issue}" .
            ?article :volume "{volume}" .
            ?article :publication_venue "{journal_id}" .
        }}
        """
        return query
    
    def getJournalArticlesInVolume(volume, journal_id):
        query = f"""
        SELECT ?article WHERE {{
            ?article :type "journal-article" .
            ?article :volume "{volume}" .
            ?article :publication_venue "{journal_id}" .
        }}
        """
        return query
    
    def getJournalArticlesInJournal(journal_id):
        query = f"""
        SELECT ?article WHERE {{
            ?article :type "journal-article" .
            ?article :publication_venue "{journal_id}" .
        }}
        """
        return query
    
    def getProceedingsByEvent(event_name):
        query = f"""
        SELECT ?proceeding WHERE {{
            ?proceeding :type "proceeding" .
            ?proceeding :event ?event .
            FILTER(contains(lcase(str(?event)), "{event_name.lower()}"))
        }}
        """
        return query
    
    def getPublicationAuthors(publication_id):
        query = f"""
        SELECT ?author WHERE {{
            ?publication :id "{publication_id}" .
            ?publication :authors ?author .
        }}
        """
        return query
    
    def getPublicationsByAuthorName(author_name):
        query = f"""
        SELECT ?publication WHERE {{
            ?publication :authors ?author .
            FILTER(contains(lcase(str(?author)), "{author_name.lower()}"))
        }}
        """
        return query
    
    def getDistinctPublisherOfPublications(publication_ids):
        query = f"""
        SELECT DISTINCT ?publisher WHERE {{
            ?publication :id ?id .
            ?publication :publisher ?publisher .
            FILTER(?id IN ({','.join([f'"{pid}"' for pid in publication_ids])}))
        }}
        """
        return query

    def getAllPublications(self):
        query = """
        PREFIX schema: <http://schema.org/>
        SELECT ?publication WHERE {
            ?publication a schema:ScholarlyArticle .
        }
        """
        results = self.execute_query(query)
        publications = [result["publication"]["value"] for result in results["results"]["bindings"]]
        return publications

    def testSimpleQuery(self):
        query = "SELECT * WHERE { ?s ?p ?o } LIMIT 5"
        results = self.execute_query(query)
        print(results)
        
# Example usage
if __name__ == "__main__":
    # Initialize GraphDatabase with Blazegraph endpoint
    print("Initializing GraphDatabase...")
    graph_db = GraphDatabase("http://localhost:9999/blazegraph/sparql", "user", "password")
    
    # Clear the Blazegraph database
    print("Clearing Blazegraph database...")
    graph_db.clear_blazegraph()

    # Initialize GraphDataProcessor
    print("Initializing GraphDataProcessor...")
    graph_data_processor = GraphDataProcessor()

    # Initialize GraphQueryProcessor
    print("Initializing GraphQueryProcessor...")
    graph_query_processor = GraphQueryProcessor(graph_db)

    # Test getAllPublications
    print("Testing getAllPublications...")
    all_publications = graph_query_processor.getAllPublications()
    #print(f"All publications: {all_publications}")

    # Upload data and get triples
    print("Uploading data and generating triples...")
    csv_path = "/Users/juanpablocasadobissone/Downloads/graph_publications.csv"
    json_path = "/Users/juanpablocasadobissone/Downloads/graph_other_data copy.json"
    triples = graph_data_processor.uploadData(csv_path, json_path)
    
    if triples is not None:
        print("Uploading triples to database...")
        graph_db.upload_triples_to_blazegraph(triples)
    else:
        print("No triples to upload.")
    print(len(triples))

    # Test getPublicationsPublishedInYear
    print("Testing getPublicationsPublishedInYear...")
    year_to_query = 2011  # Replace with the year you want to test
    publications = graph_query_processor.getPublicationsPublishedInYear(year_to_query)
    print(f"Publications published in {year_to_query}: {publications}")
    
    # Test getMostCitedPublication
    print("Testing getMostCitedPublication...")
    most_cited_publication = graph_query_processor.getMostCitedPublication()
    print(f"The most cited publication is: {most_cited_publication}")
    
    # Test getMostCitedVenue
    print("\nTesting getMostCitedVenue...")
    most_cited_venue = graph_query_processor.getMostCitedVenue()
    print(f"The most cited venue is: {most_cited_venue}")

    # Testing getVenuesByPublisherId
    print("Testing getVenuesByPublisherId...")
    publisher_id_to_query = "crossref:297"
    
    # Call the method to get the query
    sparql_query = graph_query_processor.getVenuesByPublisherId(publisher_id_to_query)
    
    # Execute the query
    #venues = graph_query_processor.execute_query(sparql_query)
    #print(f"Venues by publisher {publisher_id_to_query}: {venues}")

    # Testing getPublicationInVenue
    print("Testing getPublicationInVenue...")
    venue_id_to_query = "issn:0944-1344"  # Replace this with the venue ID you want to test
    
    # Call the method to get the DataFrame
    df_publications = graph_query_processor.getPublicationInVenue(venue_id_to_query)
    
    print(f"Publications in venue {venue_id_to_query}:")
    print(df_publications)


# In[ ]:




