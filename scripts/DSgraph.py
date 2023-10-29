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
    'venues_id': 'http://schema.org/isPartOf',  # Updated line
    'venue_type': 'http://schema.org/additionalType',
    'publisher': 'http://schema.org/publisher',
    
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
        self.sparql = SPARQLWrapper(self.endpointUrl)  
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


    #save triples to a text file
    def saveTriplesToFile(self, triples, filename):
        """
        Save triples to a text file.
        
        Parameters:
        - triples: List of triples to be saved.
        - filename: Name of the file where triples will be saved.
        """
        with open(filename, 'w') as f:
            for triple in triples:
                f.write(f"{triple}\n")
        print(f"Saved {len(triples)} triples to {filename}.")
    
    def generateTriples(self):
        """
        Generate triples from the merged DataFrame in N-Triples format.
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
                    # Check if the object has multiple comma-separated values
                    if isinstance(object_, str) and ',' in object_:
                        for single_object in object_.split(','):
                            single_object = single_object.strip()  # Remove any leading/trailing whitespace
                            triples.append(f"<{subject}> <{predicate}> \"{single_object}\" .")
                    else:
                        # Decide the formatting based on the column type
                        if col in ["venues_id", "id", "type"]:  # We assume venues_id, id, and type are URIs
                            triples.append(f"<{subject}> <{predicate}> <{object_}> .")
                        else:
                            triples.append(f"<{subject}> <{predicate}> \"{object_}\" .")
    
        print(f"Generated {len(triples)} triples.")
    
        # Write the triples to a file in N-Triples format
        with open('triples.nt', 'w') as f:
            for triple in triples:
                f.write(f"{triple}\n")
    
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
        
        for triple in triples:
            # Since the triples are already in N-Triples format, you can directly append them
            insert_query += f"{triple} "
    
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
        print(f"Executing query: {query}")  # Debug print
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        print(f"Raw result: {results}")
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
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Publication": publications})
        return df

    def getMostCitedPublication(self):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?publication (COUNT(?reference) AS ?citationCount) WHERE {{
            ?publication schema:citation ?reference .
        }} GROUP BY ?publication ORDER BY DESC(?citationCount) LIMIT 1
        """
        results = self.execute_query(query)
        if results["results"]["bindings"]:
            publication = results["results"]["bindings"][0]["publication"]["value"]
            citationCount = int(results["results"]["bindings"][0]["citationCount"]["value"])
        else:
            return None
        
        # Convert the result to a DataFrame
        df = pd.DataFrame({"Publication": [publication], "CitationCount": [citationCount]})
        return df

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
            venue = results["results"]["bindings"][0]["venue"]["value"]
            citationCount = int(results["results"]["bindings"][0]["citationCount"]["value"])
        else:
            return None
        
        # Convert the result to a DataFrame
        df = pd.DataFrame({"Venue": [venue], "CitationCount": [citationCount]})
        return df

    def getVenuesByPublisherId(self, publisher_id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?venue WHERE {{
            ?venue schema:publisher "{publisher_id}" .
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Venue": [result["venue"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getPublicationInVenue(self, venue_name):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?publication WHERE {{
            ?publication schema:isPartOf ?venue .
            FILTER (str(?venue) = "{venue_name}")
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Publication": [result["publication"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getJournalArticlesInIssue(self, journal_id, volume, issue):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?article WHERE {{
            ?article schema:isPartOf ?journal .
            ?article schema:volumeNumber "{volume}" .
            ?article schema:issueNumber "{issue}" .
            FILTER (strstarts(str(?journal), "issn:") || str(?journal) = "{journal_id}")
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Article": [result["article"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getJournalArticlesInVolume(self, journal_id, volume):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?article WHERE {{
            ?article schema:isPartOf ?journal .
            ?article schema:volumeNumber "{volume}" .
            FILTER (strstarts(str(?journal), "issn:") || str(?journal) = "{journal_id}")
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Article": [result["article"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getJournalArticlesInJournal(self, journal_id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?article WHERE {{
            ?article schema:isPartOf ?journal .
            FILTER (strstarts(str(?journal), "issn:") || str(?journal) = "{journal_id}")
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Article": [result["article"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getProceedingsByEvent(self, event_name):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?proceeding WHERE {{
            ?proceeding a schema:ScholarlyArticle .
            ?proceeding schema:isPartOf ?event .
            FILTER (regex(str(?event), "{event_name}", "i"))
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Proceeding": [result["proceeding"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getPublicationAuthors(self, publication_id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?author WHERE {{
            <{publication_id}> schema:author ?author .
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Author": [result["author"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getPublicationsByAuthorName(self, author_name):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?article ?title WHERE {{
            ?article schema:author ?author .
            ?article schema:name ?title .
            FILTER (regex(str(?author), "{author_name}", "i"))
        }}
        """
        results = self.execute_query(query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame({"Article": [result["article"]["value"] for result in results["results"]["bindings"]],
                            "Title": [result["title"]["value"] for result in results["results"]["bindings"]]})
        return df

    def getDistinctPublisherOfPublications(self, doi_list):
        # Modify this line to include angle brackets around each DOI
        doi_str = ', '.join([f'<{doi}>' for doi in doi_list])
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT DISTINCT ?publisher WHERE {{
            ?article schema:identifier ?doi .
            ?article schema:publisher ?publisher .
            FILTER (?doi IN ({doi_str}))
        }}
        """
        results = self.execute_query(query)

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

    def getISSNOfArticle(self, article_uri):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?issn WHERE {{
            <{article_uri}> schema:isPartOf ?journal .
            ?journal schema:issn ?issn .
        }}
        """
        results = self.execute_query(query)
        # Extract the ISSN from the results
        if results and 'bindings' in results['results'] and len(results['results']['bindings']) > 0:
            return results['results']['bindings'][0]['issn']['value']
        else:
            return None

    def getArticleDetails(self, article_uri):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?predicate ?object WHERE {{
            <{article_uri}> ?predicate ?object .
        }}
        """
        results = self.execute_query(query)
        return results

        
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
    csv_path = "data/graph_publications.csv"
    json_path = "data/graph_other_data copy.json"
    triples = graph_data_processor.uploadData(csv_path, json_path)
    if triples:
        graph_data_processor.saveTriplesToFile(triples, '/Users/juanpablocasadobissone/Downloads/data/triples.txt')  # save to a text file

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
    sparql_query = graph_query_processor.getVenuesByPublisherId(publisher_id_to_query)


    # Testing getPublicationInVenue
    print("Testing getPublicationInVenue...")
    venue_name_to_query = "Applied Sciences"  # Replace this with the venue name you want to test
    
    # Existing line to call the method and get the DataFrame
    df_publications = graph_query_processor.getPublicationInVenue(venue_name_to_query)
    
    # Existing line to print the DataFrame
    print(f"Publications in venue {venue_name_to_query}:")
    print(df_publications)

    # Testing getJournalArticlesInIssue
    print("Testing getJournalArticlesInIssue...")
    df_articles_issue = graph_query_processor.getJournalArticlesInIssue("issn:0219-1377", "55", "3")
    print(f"Articles in volume 55, issue 3:")
    print(df_articles_issue)
    
    # Testing getJournalArticlesInVolume
    print("Testing getJournalArticlesInVolume...")
    df_articles_volume = graph_query_processor.getJournalArticlesInVolume("issn:1570-8268", "70")
    print(f"Articles in volume 70:")
    print(df_articles_volume)

    # Testing getJournalArticlesInVolume
    print("Testing getJournalArticlesInVolume...")
    df_articles_volume = graph_query_processor.getJournalArticlesInVolume("issn:1942-4787", "11")
    print(f"Articles in volume 11:")
    print(df_articles_volume)

    # Test getJournalArticlesInJournal
    print("Testing getJournalArticlesInJournal...")
    df_articles_journal = graph_query_processor.getJournalArticlesInJournal("Acm Computing Surveys")
    print(f"Articles in journal Acm Computing Surveys:")
    print(df_articles_journal)

    # Test getProceedingsByEvent
    print("Testing getProceedingsByEvent...")
    df_proceedings_event = graph_query_processor.getProceedingsByEvent("web")
    print(f"Proceedings related to 'web':")
    print(df_proceedings_event)
    
    # Test getPublicationAuthors
    print("Testing getPublicationAuthors...")
    df_pub_authors = graph_query_processor.getPublicationAuthors("http://dx.doi.org/doi_10.1017/s0021859619000820")
    print(f"Authors of the publication:")
    print(df_pub_authors)

    print("Testing getPublicationsByAuthorName...")
    df_publications_author = graph_query_processor.getPublicationsByAuthorName("Diefenbach")
    print(f"Publications by author name partially matching 'Diefenbach':")
    print(df_publications_author)

    # Testing getDistinctPublisherOfPublications
    print("Testing getDistinctPublisherOfPublications...")
    df_publishers = graph_query_processor.getDistinctPublisherOfPublications(["doi:10.1007/978-3-030-00461-3_6"])
    print(f"Distinct publishers of specified publications:")
    print(df_publishers)
