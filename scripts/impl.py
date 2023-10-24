import os
import sqlite3
import json
import csv
import pandas as pd

class RelationalDataProcessor:
    def __init__(self):
        self.db_path = None

    def setDbPath(self, db_path):
        if os.path.exists(db_path):
            self.db_path = db_path
            return True
        else:
            print(f"Database path '{db_path}' does not exist.")
            return False

    def uploadData(self, path):
        if not self.db_path:
            print("Database path is not set. Please set the database path first.")
            return False

        if not os.path.exists(path):
            print(f"File not found: {path}")
            return False

        try:
            # Your data upload logic here
            print(f"Data uploaded from {path} to {self.db_path}")
            return True
        except Exception as e:
            print(f"Error uploading data: {str(e)}")
            return False

    def parse_json_to_dataframe(self, json_file):
        if not os.path.exists(json_file):
            print(f"JSON file not found: {json_file}")
            return None

        try:
            with open(json_file, 'r') as json_data:
                data = json.load(json_data)

            # Your DataFrame creation logic here
            # You need to define the DataFrame structure based on your data
            df = pd.DataFrame(data)

            return df
        except Exception as e:
            print(f"Error parsing JSON to DataFrame: {str(e)}")
            return None

    def parse_csv_to_dataframe(self, csv_file):
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return None

        try:
            df = pd.read_csv(csv_file)
            return df
        except Exception as e:
            print(f"Error parsing CSV to DataFrame: {str(e)}")
            return None

class RelationalQueryProcessor(RelationalDataProcessor):
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def getPublicationPublishedInYear(self, year):
        # Define the SQL query to get publications published in a specific year
        query = f"SELECT * FROM Publications WHERE publication_year = {year}"
        return pd.read_sql_query(query, self.db_connection)

    def getPublicationsByAuthorId(self, author_id):
        # Define the SQL query to get publications by author ID
        query = f"SELECT * FROM Publications WHERE author_id = '{author_id}'"
        return pd.read_sql_query(query, self.db_connection)
    def getMostCitedPublication(self):
        # Define the SQL query to get the most cited publication
        query = "SELECT * FROM Publications ORDER BY citation_count DESC LIMIT 1"
        return pd.read_sql_query(query, self.db_connection)

    def getMostCitedVenue(self):
        # Define the SQL query to get the most cited venue
        query = """
            SELECT Venues.venue_id, Venues.name, SUM(Publications.citation_count) AS total_citations
            FROM Venues
            JOIN Publications ON Venues.venue_id = Publications.venue_id
            GROUP BY Venues.venue_id, Venues.name
            ORDER BY total_citations DESC
            LIMIT 1
        """
        return pd.read_sql_query(query, self.db_connection)
    def getVenuesByPublisherId(self, publisher_id):
        # Define the SQL query to get venues by publisher ID
        query = f"SELECT * FROM Venues WHERE publisher_id = '{publisher_id}'"
        return pd.read_sql_query(query, self.db_connection)

    def getPublicationInVenue(self, venue_id):
        # Define the SQL query to get publications in a specific venue
        query = f"SELECT * FROM Publications WHERE venue_id = '{venue_id}'"
        return pd.read_sql_query(query, self.db_connection)

    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        # Define the SQL query to get journal articles in a specific issue
        query = f"""
            SELECT * FROM Publications
            WHERE publication_type = 'Journal Article'
            AND issue = '{issue}'
            AND volume = '{volume}'
            AND journal_id = '{journal_id}'
        """
        def getJournalArticlesInVolume(self, volume, journal_id):
        # Define the SQL query to get journal articles in a specific volume
        query = f"""
            SELECT * FROM Publications
            WHERE publication_type = 'Journal Article'
            AND volume = '{volume}'
            AND journal_id = '{journal_id}'
        """
        return pd.read_sql_query(query, self.db_connection)

    def getJournalArticlesInJournal(self, journal_id):
        # Define the SQL query to get journal articles in a specific journal
        query = f"""
            SELECT * FROM Publications
            WHERE publication_type = 'Journal Article'
            AND journal_id = '{journal_id}'
        """
        return pd.read_sql_query(query, self.db_connection)

    def getProceedingsByEvent(self, event_partial_name):
        # Define the SQL query to get proceedings by event name
        query = f"""
            SELECT * FROM Publications
            WHERE publication_type = 'Conference Proceedings'
            AND event_name LIKE '%{event_partial_name}%'
        """
        return pd.read_sql_query(query, self.db_connection)

    def getPublicationAuthors(self, publication_id):
        # Define the SQL query to get authors of a specific publication
        query = f"""
            SELECT Authors.author_id, Authors.family, Authors.given
            FROM Authors
            JOIN Authorship ON Authors.author_id = Authorship.author_id
            WHERE Authorship.publication_id = '{publication_id}'
        """
        return pd.read_sql_query(query, self.db_connection)

    def getPublicationByAuthorName(self, author_partial_name):
        # Define the SQL query to get publications by author name
        query = f"""
            SELECT Publications.*
            FROM Publications
            JOIN Authorship ON Publications.publication_id = Authorship.publication_id
            JOIN Authors ON Authorship.author_id = Authors.author_id
            WHERE Authors.family LIKE '%{author_partial_name}%'
            OR Authors.given LIKE '%{author_partial_name}%'
        """
        return pd.read_sql_query(query, self.db_connection)

    def getDistinctPublishersOfPublications(self, pub_id_list):
        # Define the SQL query to get distinct publishers of a list of publications
        pub_id_list_str = ', '.join([f"'{pub_id}'" for pub_id in pub_id_list])
        query = f"""
            SELECT DISTINCT Publishers.publisher_id, Publishers.name
            FROM Publishers
            JOIN Publications ON Publishers.publisher_id = Publications.publisher_id
            WHERE Publications.publication_id IN ({pub_id_list_str})
        """
        return pd.read_sql_query(query, self.db_connection)
    
class RelationalProcessor(RelationalQueryProcessor):
    def __init__(self, db_connection):
        super().__init__(db_connection)

    # Implement other query methods as described

if __name__ == "__main__":
    # Create an instance of RelationalDataProcessor
    data_processor = RelationalDataProcessor()
    data_processor.setDbPath("./pabloale.db")  # Setting the database path
    # Use data_processor for data processing with RelationalDataProcessor methods

    # Create an instance of RelationalQueryProcessor
    query_processor = RelationalQueryProcessor(data_processor.db_connection)  # Pass the database connection from the data_processor
    # Use query_processor for data querying with RelationalQueryProcessor methods

    # Create an instance of RelationalProcessor
    processor = RelationalProcessor(query_processor.db_connection)  # Pass the database connection from the query_processor
    # Use processor for data processing with RelationalProcessor methods

    # Now you can use each variable independently:
    # data_processor for data processing, query_processor for querying, and processor for additional processing

    # Setting the database path
    success = data_processor.setDbPath("./pabloale.db")
    if success:
        print("Database path set successfully")

    # Uploading data
    data_path = "/data/data_to_upload.txt"  # Path to the data file
    upload_success = data_processor.uploadData(data_path)
    if upload_success:
        print("Data upload successful")
    else:
        print("Data upload failed")

    # Parsing JSON to DataFrame
    json_file = "/data/relational_other_data.json"
    json_df = data_processor.parse_json_to_dataframe(json_file)
    if json_df is not None:
        print("JSON data as DataFrame:")
        print(json_df)

    # Parsing CSV to DataFrame
    csv_file = "/data/relational_publications.csv"
    csv_df = data_processor.parse_csv_to_dataframe(csv_file)
    if csv_df is not None:
        print("CSV data as DataFrame:")
        print(csv_df)

    # Query example: Get publications published in 2022
    publications_2022 = query_processor.getPublicationPublishedInYear(2022)
    if not publications_2022.empty:
        print("Publications published in 2022:")
        print(publications_2022)
