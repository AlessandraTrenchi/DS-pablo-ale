#!/usr/bin/env python
# coding: utf-8

# In[69]:


import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, JSON
import json
import sqlite3


# RelationalDataProcessor class definition
class RelationalDataProcessor: 
    def __init__(self, db_path): #db_path is the path to the SQLite database file.
        self.dbPath = db_path 
        self.merged_df = None  # To store the merged DataFrame
        self.conn = None  # To store the database connection
        
    def setDbPath(self, new_path): #allows changes the path
        self.dbPath = new_path

    def ensureDbConnection(self):
        if self.conn is None:
            print("Reconnecting to the database...")
            self.connectDb() #tries reconnecting
        print(f"Database connection: {self.conn}")

    def connectDb(self):
        print("Attempting to connect to the database...")
        self.conn = sqlite3.connect(self.dbPath)
        print("Connected to the database.")
        print(f"self.conn: {self.conn}")

    def closeDb(self):
        if self.conn:
            self.conn.close()
    
    def uploadData(self, csv_file, json_file):
        self.ensureDbConnection()
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
        
        # Step 2: Merge Data into pandas df_csv
        for doi, authors in data_json['authors'].items(): #starts the iteration through the items (key-value pairs)
            author_str = ', '.join([f"{a['given']} {a['family']} ({a['orcid']})" for a in authors])
            # author_str = "Peroni, Silvio, 0000-"
            family_str = ', '.join([a['family'] for a in authors])
            given_str = ', '.join([a['given'] for a in authors])
            orcid_str = ', '.join([a['orcid'] for a in authors])
            
            #update the df by locating the where the id column matches the current doi
            #for each column assigns the corresponding merged string
            df_csv.loc[df_csv['id'] == doi, 'authors'] = author_str  
            df_csv.loc[df_csv['id'] == doi, 'family'] = family_str
            df_csv.loc[df_csv['id'] == doi, 'given'] = given_str
            df_csv.loc[df_csv['id'] == doi, 'orcid'] = orcid_str
        
        # Step 3: Normalize Data within df_csv
        #doi: publication identifier, venues: list of venue ids
        for doi, venues in data_json['venues_id'].items(): #iterates through the items in venue_id dict
            df_csv.loc[df_csv['id'] == doi, 'venues_id'] = ', '.join(venues) #locates the rows where 'id' matches the current 'doi', the identifiers are joined into a single string assigned to the venues_id df
        
        for doi, refs in data_json['references'].items():
            df_csv.loc[df_csv['id'] == doi, 'references'] = ', '.join(refs)
        
        # Step 4: Upload to Database
        conn = sqlite3.connect(self.dbPath)
        df_csv.to_sql('Publications', conn, if_exists='replace', index=False) #transfer the content of df_csv to sqlite database
        conn.close()
    
        # Store the merged DataFrame
        self.merged_df = df_csv
    
        # Save the merged DataFrame to a local CSV file
        self.merged_df.to_csv('/Users/juanpablocasadobissone/Downloads/data/master_table.db', index=False)

# RelationalQueryProcessor class definition
class RelationalQueryProcessor(RelationalDataProcessor):
    def __init__(self, db_path, db_connection):
        super().__init__(db_path)
        self.db_connection = db_connection

    def setDbPath(self, new_path):
        self.dbPath = new_path

    def getDbPath(self):  # Add this method
        return self.dbPath

    def getPublicationPublishedInYear(self, year):
        # Define the SQL query to get publications published in a specific year
        query = f"SELECT * FROM Publications WHERE publication_year = {year}"
        return pd.read_sql_query(query, self.db_connection)

    def getPublicationsByAuthorId(self, author_id):
        if self.conn is None:
            print("Reconnecting to the database...")
            self.connectDb()
        print(f"Database connection in getPublicationsByAuthorId: {self.conn}")  # Debugging line
        query = f"SELECT * FROM publications WHERE authors LIKE '%{author_id}%'"
        df = pd.read_sql_query(query, self.conn)  
        return df

    
    def getMostCitedPublication(self):
        self.ensureDbConnection()
        # Define the SQL query to get the most cited publication based on the 'references' column
        query = """SELECT *, 
                          CASE 
                            WHEN "references" IS NULL OR "references" = '' THEN 0
                            ELSE LENGTH("references") - LENGTH(REPLACE("references", ',', '')) + 1
                          END AS num_references
                   FROM Publications
                   WHERE 
                       CASE 
                         WHEN "references" IS NULL OR "references" = '' THEN 0
                         ELSE LENGTH("references") - LENGTH(REPLACE("references", ',', '')) + 1
                       END = 
                       (
                           SELECT MAX(
                               CASE 
                                 WHEN "references" IS NULL OR "references" = '' THEN 0
                                 ELSE LENGTH("references") - LENGTH(REPLACE("references", ',', '')) + 1
                               END
                           ) 
                           FROM Publications
                       )
                   ORDER BY num_references DESC;"""
        return pd.read_sql_query(query, self.db_connection)

    def getMostCitedVenue(self):
        self.ensureDbConnection()
        query = """
        SELECT venues_id,
               SUM( 
                   CASE
                       WHEN "references" IS NOT NULL AND "references" != '' THEN LENGTH("references") - LENGTH(REPLACE("references", ',', '')) + 1
                       ELSE 0
                   END
               ) AS total_citations
        FROM Publications
        GROUP BY venues_id
        ORDER BY total_citations DESC;
        """
        #calculates the total number of citations for each venue (sum)
        #counts the number of commas in the references column
        #group by venue_id to get a summary for each unique venue
        #orders descendingly 
        return pd.read_sql_query(query, self.db_connection) 
        # Returns the results as a Pandas DataFrame containing information about venues and their total citation

    def getVenuesByPublisherId(self, publisher_id):
        self.ensureDbConnection()
        query = f"""SELECT DISTINCT publication_venue, venue_type
                    FROM Publications
                    WHERE publisher = '{publisher_id}'"""
        return pd.read_sql_query(query, self.db_connection)


    def getPublicationInVenue(self, venue_id):
        self.ensureDbConnection()
        query = f"""SELECT * FROM Publications WHERE venues_id LIKE '%{venue_id}%'"""
        return pd.read_sql_query(query, self.db_connection)


    def getJournalArticlesInIssue(self, venue_id, issue, volume):
        self.ensureDbConnection()
        query = f"""SELECT * FROM Publications WHERE type = 'journal-article' AND venues_id LIKE '%{venue_id}%' AND issue = '{issue}' AND volume = '{volume}'"""
        return pd.read_sql_query(query, self.db_connection)


    def getJournalArticlesInVolume(self, volume, journal_id):
        self.ensureDbConnection()
        query = f"""SELECT * FROM Publications 
                    WHERE volume = '{volume}' 
                    AND venues_id LIKE '%{journal_id}%'
                    AND type = 'journal-article';"""
        return pd.read_sql_query(query, self.db_connection)


    def getJournalArticlesInJournal(self, journal_id):
        self.ensureDbConnection()
        query = f"""SELECT * FROM Publications 
                    WHERE venues_id LIKE '%{journal_id}%' 
                    AND type = 'journal-article';"""
        return pd.read_sql_query(query, self.db_connection)


    def getProceedingsByEvent(self, event_name):
        self.ensureDbConnection()
        query = f"""SELECT * FROM Publications 
                    WHERE LOWER(event) LIKE '%{event_name.lower()}%' 
                    AND type = 'proceedings';"""
        return pd.read_sql_query(query, self.db_connection)

    
    def getPublicationAuthors(self, doi):
        self.ensureDbConnection()
        query = f"""SELECT authors FROM Publications WHERE id = '{doi}'"""
        return pd.read_sql_query(query, self.db_connection)


    def getPublicationsByAuthorName(self, name):
        self.ensureDbConnection()
        name = name.lower()
        query = f"""SELECT * FROM Publications 
                    WHERE LOWER(authors) LIKE '%{name}%'"""
        return pd.read_sql_query(query, self.db_connection)


    def getDistinctPublisherOfPublications(self, dois):
        self.ensureDbConnection()
        doi_list = ', '.join([f"'{doi}'" for doi in dois])
        query = f"""SELECT DISTINCT publisher FROM Publications 
                    WHERE id IN ({doi_list})"""
        return pd.read_sql_query(query, self.db_connection)

# TriplestoreDataProcessor class definition
class TriplestoreDataProcessor:
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

# TriplestoreQueryProcessor class definition
class TriplestoreQueryProcessor:
    def __init__(self, blazegraph_db):
        self.blazegraph_db = blazegraph_db.getEndpointUrl()
        self.sparql = SPARQLWrapper(self.blazegraph_db)
        self.sparql.setReturnFormat(JSON)

    def setEndpointUrl(self, new_url):
        self.blazegraph_db = new_url
        self.sparql = SPARQLWrapper(self.blazegraph_db)
        self.sparql.setReturnFormat(JSON)

    def getEndpointUrl(self):
        return self.blazegraph_db

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

    def getPublicationsByAuthorId(self, author_id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?article ?title WHERE {{
            ?article schema:author ?author .
            ?article schema:name ?title .
            FILTER (regex(str(?author), "{author_id}", "i"))
        }}
        """
        results = self.execute_query(query)
        return results

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

    def getPublicationInVenue(self, issn):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?publication WHERE {{
            ?publication schema:isPartOf ?venue .
            FILTER (str(?venue) = "issn:{issn}")
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

    def getJournalArticlesInJournal(self, issn):
        query = f"""
        PREFIX schema: <http://schema.org/>
        SELECT ?article WHERE {{
            ?article schema:isPartOf "issn:{issn}" .
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
        

class GenericQueryProcessor:
    def __init__(self):
        self.query_processors = []

    def addQueryProcessor(self, qp):
        self.query_processors.append(qp)

    def getPublicationsPublishedInYear(self, year):
        results = []
        for qp in self.query_processors:
            if hasattr(qp, 'getPublicationsPublishedInYear'):
                qp_results = qp.getPublicationsPublishedInYear(year)
            elif hasattr(qp, 'getPublicationPublishedInYear'):
                qp_results = qp.getPublicationPublishedInYear(year)
            else:
                continue  # Skip this query processor if it doesn't have the method
    
            if isinstance(qp_results, list):
                results.extend(qp_results)
            elif isinstance(qp_results, pd.DataFrame):
                results.extend(qp_results.to_dict('records'))
        return results

    def getPublicationsByAuthorId(self, author_id):
        results = []
        for qp in self.query_processors:
            qp_results = qp.getPublicationsByAuthorId(author_id)
            if isinstance(qp_results, list):
                results.extend(qp_results)
            elif isinstance(qp_results, pd.DataFrame):
                results.extend(qp_results.to_dict('records'))
        return results

    def getMostCitedPublication(self):
        most_cited = None
        max_citations = 0
        for qp in self.query_processors:
            qp_results = qp.getMostCitedPublication()
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            # Extract the citation count and compare
            for record in qp_results:
                citation_count = record.get('CitationCount', 0)  # Replace 'CitationCount' with the actual column name if different
                if citation_count > max_citations:
                    most_cited = record
                    max_citations = citation_count
        
        return most_cited


    def getMostCitedVenue(self):
        most_cited_venue = None
        max_citations = 0
        for qp in self.query_processors:
            qp_results = qp.getMostCitedVenue()
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            # Extract the citation count and compare
            for record in qp_results:
                citation_count = record.get('total_citations', 0)  # Replace 'total_citations' with the actual column name if different
                if citation_count > max_citations:
                    most_cited_venue = record.get('venues_id')  # Replace 'venues_id' with the actual column name if different
                    max_citations = citation_count
        
        return most_cited_venue

    def getVenuesByPublisherId(self, publisher_id):
        all_venues = []
        for qp in self.query_processors:
            qp_results = qp.getVenuesByPublisherId(publisher_id)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            # Extend the list of all venues
            all_venues.extend(qp_results)
        
        # Convert the list of dictionaries to a DataFrame
        return pd.DataFrame(all_venues)

    def getPublicationInVenue(self, venue_id):
        results = []
        for qp in self.query_processors:
            # Check if the query processor has the method 'getPublicationInVenue'
            if hasattr(qp, 'getPublicationInVenue'):
                qp_results = qp.getPublicationInVenue(venue_id)
                
                # Standardize the results
                if qp_results is None:
                    continue
                elif isinstance(qp_results, pd.DataFrame):
                    qp_results = qp_results.to_dict('records')
                
                results.extend(qp_results)
        
        return results

    def getJournalArticlesInIssue(self, journal_id, volume, issue):
        results = []
        for qp in self.query_processors:
            qp_results = qp.getJournalArticlesInIssue(journal_id, volume, issue)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            elif isinstance(qp_results, dict):  # Assuming the graph DB returns a dictionary
                qp_results = [qp_results]  # Convert it to a list of dictionaries
            
            results.extend(qp_results)
        
        # Convert the list of dictionaries back to a DataFrame for final output
        final_df = pd.DataFrame(results)
        return final_df

    def getJournalArticlesInVolume(self, journal_id, volume):
        results = []
        for qp in self.query_processors:
            qp_results = qp.getJournalArticlesInVolume(journal_id, volume)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            elif isinstance(qp_results, dict):  # Assuming the graph DB returns a dictionary
                qp_results = [qp_results]  # Convert it to a list of dictionaries
            
            results.extend(qp_results)
        
        return results

    def getJournalArticlesInJournal(self, journal_id):
        articles = []
        for qp in self.query_processors:
            qp_results = qp.getJournalArticlesInJournal(journal_id)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            # Extend the list of articles
            articles.extend(qp_results)
        
        return articles

    def getProceedingsByEvent(self, event_name):
        results = []
        for qp in self.query_processors:
            qp_results = qp.getProceedingsByEvent(event_name)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            results.extend(qp_results)
        
        return results

    def getPublicationAuthors(self, doi):
        authors_list = []
        for qp in self.query_processors:
            qp_results = qp.getPublicationAuthors(doi)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            # Extract the authors and add to the list
            for record in qp_results:
                authors = record.get('authors', [])  # Replace 'authors' with the actual column name if different
                if authors:
                    authors_list.extend(authors.split(', '))  # Assuming authors are comma-separated
        
        return list(set(authors_list))  # Remove duplicates

    def getPublicationsByAuthorName(self, author_name):
        results = []
        for qp in self.query_processors:
            qp_results = qp.getPublicationsByAuthorName(author_name)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results.to_dict('records')
            
            results.extend(qp_results)
        
        return results

    def getDistinctPublisherOfPublications(self, dois):
        distinct_publishers = set()
        
        for qp in self.query_processors:
            qp_results = qp.getDistinctPublisherOfPublications(dois)
            
            # Standardize the results
            if qp_results is None:
                continue
            elif isinstance(qp_results, pd.DataFrame):
                qp_results = qp_results['publisher'].tolist()  # Assuming the column name is 'publisher'
            elif isinstance(qp_results, dict):  # Assuming the result is a dictionary for the graph DB
                qp_results = [binding['publisher']['value'] for binding in qp_results['results']['bindings']]
            
            # Add to the set of distinct publishers
            distinct_publishers.update(qp_results)
        
        return list(distinct_publishers)

# Main code
if __name__ == "__main__":
    rel_path = "relational.db"
    rel_dp = RelationalDataProcessor(rel_path)
    rel_dp.setDbPath(rel_path)
    rel_dp.connectDb() 
    print("Initializing and connecting to RelationalDataProcessor...")

    
    # ... (existing code for uploading data)
    
    csv_file = '/data/relational_publications.csv'
    json_file = '/data/relational_other_data.json'
    rel_dp.uploadData(csv_file, json_file)  
    print("Uploading data to RelationalDataProcessor...")

    grp_endpoint = "http://localhost:9999/blazegraph/sparql"
    grp_dp = TriplestoreDataProcessor()
    grp_dp.setEndpointUrl(grp_endpoint)
    print("Initializing TriplestoreDataProcessor...")
    
    # ... (existing code for uploading data)
    
    print("Uploading data and generating triples...")
    csv_path = "/data/graph_publications.csv"
    json_path = "/data/graph_other_data copy.json"
    triples = graph_data_processor.uploadData(csv_path, json_path)
    
    if triples:
        graph_data_processor.saveTriplesToFile(triples, '/Users/juanpablocasadobissone/Downloads/data/triples.txt')  # save to a text file
        print("Triples saved to file.")
    
    if triples is not None:
        print("Uploading triples to database...")
        graph_db.upload_triples_to_blazegraph(triples)
    else:
        print("No triples to upload.")
    print(len(triples))

    rel_qp = RelationalQueryProcessor(rel_path, rel_dp.conn)
    print("Connecting to the database...")

    grp_qp = TriplestoreQueryProcessor(grp_dp)
    grp_qp.setEndpointUrl(grp_endpoint)

    generic = GenericQueryProcessor()
    generic.addQueryProcessor(rel_qp)
    generic.addQueryProcessor(grp_qp)
    # ... (existing code for querying)

    #Call the getPublicationsPublishedInYear method for the year 2018
    #publications_2018 = generic.getPublicationsPublishedInYear(2018)
    #print("Publications in 2018:", publications_2018)

    #Call the getPublicationsByAuthorId method for a specific author ID
    #author_id = "0000-0003-0530-4305"
    #publications_by_author = generic.getPublicationsByAuthorId(author_id)
    #print(f"Publications by author {author_id}:", publications_by_author)

    # Call the getMostCitedPublication method
    #most_cited_publication = generic.getMostCitedPublication()
    #print(f"The most cited publication is: {most_cited_publication}")

    #most_cited_venue = generic.getMostCitedVenue()
    #print(f"The most cited venue is: {most_cited_venue}")

    #publisher_id = "crossref:297"
    #venues_by_publisher = generic.getVenuesByPublisherId(publisher_id)
    #print(f"Venues by publisher {publisher_id}:")
    #print(venues_by_publisher)

    #venue_id = "0014-5793"  # Replace with the actual venue ID
    #publications_in_venue = generic.getPublicationInVenue(venue_id)
    #print(f"Publications in venue {venue_id}: {publications_in_venue}")

    #journal_id = "issn:0219-1377"
    #volume = "55"
    #issue = "3"
    
    #journal_articles = generic.getJournalArticlesInIssue(journal_id, volume, issue)
    #print(f"Journal articles in issue {issue} of volume {volume} of journal {journal_id}:")
    #print(journal_articles)

    #journal_id = "issn:1942-4787"
    #volume = "11"
    
    #journal_articles_in_volume = generic.getJournalArticlesInVolume(journal_id, volume)
    #print(f"Journal articles in volume {volume} of journal {journal_id}:")
    #print(journal_articles_in_volume)

    #journal_id = "0360-0300"
    #articles_in_journal = generic.getJournalArticlesInJournal(journal_id)
    #print(f"Articles in journal {journal_id}: {articles_in_journal}")

    #proceedings_by_event = generic.getProceedingsByEvent("web")
    #print(f"Proceedings related to 'web': {proceedings_by_event}")

    #doi = "http://dx.doi.org/doi_10.1017/s0021859619000820"
    #publication_authors = generic.getPublicationAuthors(doi)
    #print(f"Authors of the publication with DOI {doi}: {publication_authors}")

    #publications_by_author_name = generic.getPublicationsByAuthorName("Diefenbach")
    #print(f"Publications by authors with name partially matching 'Diefenbach':")
    #print(publications_by_author_name)

    dois = ["doi:10.1007/978-3-030-00461-3_6", "doi:10.3390/ijfs9030035"]
    distinct_publishers = generic.getDistinctPublisherOfPublications(dois)
    print(f"Distinct publishers for the given DOIs are: {distinct_publishers}")




# In[ ]:




