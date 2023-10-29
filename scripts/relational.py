#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd
import json
import sqlite3

class RelationalDataProcessor:
    def __init__(self, db_path):
        self.dbPath = db_path
        self.merged_df = None  # To store the merged DataFrame
        
    def setDbPath(self, new_path):
        self.dbPath = new_path
    

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
        
        # Step 4: Upload to Database
        conn = sqlite3.connect(self.dbPath)
        df_csv.to_sql('Publications', conn, if_exists='replace', index=False)
        conn.close()
    
        # Store the merged DataFrame
        self.merged_df = df_csv
    
        # Save the merged DataFrame to a local CSV file
        self.merged_df.to_csv('/Users/juanpablocasadobissone/Downloads/data/master_table.db', index=False)


# Initialize
processor = RelationalDataProcessor('your_database_path.db')

# Upload data
processor.uploadData('/Users/juanpablocasadobissone/Downloads/data/relational_publications.csv', '/Users/juanpablocasadobissone/Downloads/data/relational_other_data.json')

class RelationalDatabase:
    def __init__(self, db_path):
        self.dbPath = db_path
    
    def connect(self):
        self.conn = sqlite3.connect(self.dbPath)
    
    def execute_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
    
    def close(self):
        self.conn.close()
    
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

    def getPublicationsByAuthorId(author_id):
        query = f"SELECT * FROM publications WHERE authors LIKE '%{orcid}%'"
        df = pd.read_sql_query(query, conn)
        return df

    
    def getMostCitedPublication(self):
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



    def getMostCitedVenueSQL(self):
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
    return pd.read_sql_query(query, self.db_connection)


    def getVenuesByPublisherId(self, publisher_id):
        query = f"""SELECT DISTINCT publication_venue, venue_type
                    FROM Publications
                    WHERE publisher = '{publisher_id}'"""
        return pd.read_sql_query(query, self.db_connection)


    def getPublicationInVenue(self, venue_id):
        query = f"""SELECT * FROM Publications WHERE venues_id LIKE '%{venue_id}%'"""
        return pd.read_sql_query(query, self.db_connection)


    def getJournalArticlesInIssue(self, venue_id, issue, volume):
        query = f"""SELECT * FROM Publications WHERE type = 'journal-article' AND venues_id LIKE '%{venue_id}%' AND issue = '{issue}' AND volume = '{volume}'"""
        return pd.read_sql_query(query, self.db_connection)


    def getJournalArticlesInVolume(self, volume, journal_id):
        query = f"""SELECT * FROM Publications 
                    WHERE volume = '{volume}' 
                    AND venues_id LIKE '%{journal_id}%'
                    AND type = 'journal-article';"""
        return pd.read_sql_query(query, self.db_connection)


    def getJournalArticlesInJournal(self, journal_id):
        query = f"""SELECT * FROM Publications 
                    WHERE venues_id LIKE '%{journal_id}%' 
                    AND type = 'journal-article';"""
        return pd.read_sql_query(query, self.db_connection)


    def getProceedingsByEvent(self, event_name):
        query = f"""SELECT * FROM Publications 
                    WHERE LOWER(event) LIKE '%{event_name.lower()}%' 
                    AND type = 'proceedings';"""
        return pd.read_sql_query(query, self.db_connection)

    
    def getPublicationAuthors(self, doi):
        query = f"""SELECT authors FROM Publications WHERE id = '{doi}'"""
        return pd.read_sql_query(query, self.db_connection)


    def getPublicationsByAuthorName(self, name):
        name = name.lower()
        query = f"""SELECT * FROM Publications 
                    WHERE LOWER(authors) LIKE '%{name}%'"""
        return pd.read_sql_query(query, self.db_connection)


    def getDistinctPublisherOfPublications(self, dois):
        doi_list = ', '.join([f"'{doi}'" for doi in dois])
        query = f"""SELECT DISTINCT publisher FROM Publications 
                    WHERE id IN ({doi_list})"""
        return pd.read_sql_query(query, self.db_connection)


def main():
    # Initialize
    processor = RelationalDataProcessor('/Users/juanpablocasadobissone/Downloads/data/masters_table.db')

    # Upload data
    csv_path = '/Users/juanpablocasadobissone/Downloads/data/relational_publications.csv'
    json_path = '/Users/juanpablocasadobissone/Downloads/data/relational_other_data.json'
    processor.uploadData(csv_path, json_path)

    # Initialize Query Processor
    db_path = '/Users/juanpablocasadobissone/Downloads/data/masters_table.db'
    db_connection = sqlite3.connect(db_path)
    query_processor = RelationalQueryProcessor(db_path, db_connection)
   
    # Confirm that the database path has been set correctly
    print(f"Database path is set to: {query_processor.getDbPath()}")  # Use getDbPath here

    # Test a query
    df = query_processor.getPublicationPublishedInYear(2020)
    print(df.head())

if __name__ == '__main__':
    main()


# In[ ]:




