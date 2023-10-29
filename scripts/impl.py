import os
import sqlite3
import json
import csv
import pandas as pd

#data model classes
class Venue:
    def __init__(self, venues_id, identifiable_entity_id, title, type):
        self.venues_id = venues_id
        self.identifiable_entity_id = identifiable_entity_id
        self.title = title
        self.type = type
        self.publishers = set()  # Use a set to store related Organization objects

    def getTitle(self) -> str:
        return self.title

    def addPublisher(self, publisher):
        self.publishers.add(publisher)

    def getPublishers(self) -> set['Organization']:
        return self.publishers

    def setIdentifiableEntity(self, identifiable_entity):
        self.identifiable_entity = identifiable_entity

    def getIdentifiableEntity(self) -> 'IdentifiableEntity':
        return self.identifiable_entity


class Publication:
    def __init__(self, publication_id, title, type, publication_year, issue, volume,
                 chapter, publication_venue, venue_type, publisher_id, event_id):
        self.publication_id = publication_id
        self.title = title
        self.type = type
        self.publication_year = publication_year
        self.issue = issue
        self.volume = volume
        self.chapter = chapter
        self.publication_venue = publication_venue
        self.venue_type = venue_type
        self.publisher_id = publisher_id
        self.event_id = event_id
        self.cited_publications = []
        self.authors = set()
        self.publication_venue = None
        
    def getPublicationYear(self) -> int or None:
        if isinstance(self.publication_year, int):
            return self.publication_year
        else:
            return None

    def getTitle(self) -> str:
        return self.title

    def getCitedPublications(self) -> list['Publication']:
        # return the list of cited publications for this publication
        return self.cited_publications  
    def addCitedPublication(self, publication):
        self.cited_publications.append(publication)

    def getPublicationVenue(self) -> Venue or None: #retrieve information about a publication, return None if the Venue is not set
        """
        Retrieve the publication venue associated with this publication.

        Returns:
            Venue: The Venue object representing the publication venue.
        """
        return self.publication_venue
    
    def addPublicationVenue(self, venue): #associate or link one publication to another
        """
        Associate a publication with a publication venue.

        Args:
            venue (Venue): The Venue object representing the publication venue.
        """
        self.publication_venue = venue

    def getAuthors(self) -> set['Person']:
        #return the set of authors associated with this publication
        return self.authors  
    
    def addAuthor(self, author):
        self.authors.add(author)


class IdentifiableEntity:
    def __init__(self, identifiable_entity_id):
        self.identifiable_entity_id = identifiable_entity_id
    
    def getIds(self) -> list [str]: #Returns a list containing the ID of this entity
        return [self.identifiable_entity_id] #Retrieve a list containing the ID of this identifiable entity.


class Person:
    def __init__(self, person_id, identifiable_entity_id, given_name, family_name):
        self.person_id = person_id
        self.identifiable_entity_id = identifiable_entity_id
        self.given_name = given_name
        self.family_name = family_name

    def getGivenName (self) -> str:
        return self.given_name
    
    def getFamilyName(self) -> str:
        return self.family_name
    
class Organization:
    def __init__(self, organization_id, identifiable_entity_id, name):
        self.organization_id = organization_id
        self.identifiable_entity_id = identifiable_entity_id
        self.name = name

    def getName(self) -> str:
        return self.name

class BookChapter:
    def __init__(self, book_chapter_id, publication_id, chapter_number):
        self.book_chapter_id = book_chapter_id
        self.publication_id = publication_id
        self.chapter_number = chapter_number

    def getChapterNumber(self) -> int:
        return self.chapter_number

class Book:
    def __init__(self, book_id, venue=None):
        self.book_id = book_id
        self.venue = venue  # Establish the relationship with Venue (optional)

    def setVenue(self, venue: 'Venue'):
        self.venue = venue

    def getVenue(self) -> 'Venue':
        return self.venue


class JournalArticle:
    def __init__(self, journal_article_id, publication_id, issue: str = None, volume: str = None):
        self.journal_article_id = journal_article_id
        self.publication_id = publication_id  # Make sure the attribute names match the SQL column names
        self.issue = issue
        self.volume = volume
        self.publication = None  # Add a reference to the associated Publication

    def getIssue(self) -> str or None:
        if isinstance(self.issue, str):
            return self.issue
        else:
            return None
        
    def getVolume(self) -> str or None:
        if isinstance(self.volume, str):
            return self.volume
        else:
            return None

    def setPublication(self, publication: 'Publication'):
        self.publication = publication

    def getPublication(self) -> 'Publication':
        return self.publication




class Journal:
    def __init__(self):
        self.venue = None

    def setVenue(self, venue: 'Venue'):
        self.venue = venue

    def getVenue(self) -> 'Venue':
        return self.venue


class Proceedings:
    def __init__(self, proceedings_id, event, venue_id=None):
        self.proceedings_id = proceedings_id
        self.event = event
        self.venue_id = venue_id  # Establish the relationship with Venue (optional)

    def getEvent(self) -> str:
        return self.event

    def getVenueID(self) -> int:
        return self.venue_id


class ProceedingsPaper:
    def __init__(self, proceedings_id, publication_id):
        self.proceedings_id = proceedings_id
        self.publication_id = publication_id

    def getProceedingsID(self) -> int:
        return self.proceedings_id

    def getPublicationID(self) -> str:
        return self.publication_id


class RelationalDataProcessor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_connection = None

    def openConnection(self):
        try:
            # Connect to the SQLite database
            self.db_connection = sqlite3.connect(self.db_path)
            return True
        except Exception as e:
            print(f"Error while connecting to the database: {str(e)}")
            return False

    def closeConnection(self):
        if self.db_connection:
            self.db_connection.close()

    def uploadData(self, path: str) -> bool:
        if not self.db_connection: #checks if the database connection is open
            if not self.openConnection():
                return False

        try:
            # Depending on the file extension, call the appropriate data processing function
            if path.endswith(".json"):
                extracted_data = self.parse_json(path) #calling parse.json function
               

            elif path.endswith(".csv"):
                self.parse_csv(path)
                # Perform the data upload operation here, you can process the CSV data as needed

            # Commit the changes to the database
            self.db_connection.commit()

            return True  # Return True if the operation is successful
        except Exception as e:
            # Handle any exceptions or errors that may occur during the upload
            print(f"Error during data upload: {str(e)}")
            return False  # Return False to indicate that the operation failed

class RelationalProcessor(RelationalDataProcessor):
    def __init__(self, db_connection):
        super().__init__(db_connection)
        self.db_path = None  # Initialize db_path as None or set a default path

    def getDbPath(self) -> str: #retrieves the path and returns it as a string
        return self.db_path 

    def setDbPath(self, path: str) -> bool:
        self.db_path = path
        return True

class RelationalQueryProcessor(RelationalDataProcessor):
    def __init__(self, db_path, db_connection):
        super().__init__(db_path)
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
            SELECT Venues.venues_id, Venues.name, SUM(Publications.citation_count) AS total_citations
            FROM Venues
            JOIN Publications ON Venues.venues_id = Publications.venues_id
            GROUP BY Venues.venues_id, Venues.name
            ORDER BY total_citations DESC
            LIMIT 1
        """
        return pd.read_sql_query(query, self.db_connection)

    def getVenuesByPublisherId(self, publisher_id):
        # Define the SQL query to get venues by publisher ID
        query = f"SELECT * FROM Venues WHERE publisher_id = '{publisher_id}'"
        return pd.read_sql_query(query, self.db_connection)

    def getPublicationInVenue(self, venues_id):
        # Define the SQL query to get publications in a specific venue
        query = f"SELECT * FROM Publications WHERE venues_id = '{venues_id}'"
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
        return pd.read_sql_query(query, self.db_connection)

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
   def __init__(self, db_path, db_connection):
        super().__init__(db_path, db_connection)
        self.db_path = None  # Initialize db_path as None or set a default path
        
        def getDbPath(self) -> str:
        # Implement the method to get the database path
        # Return the path as a string
          return self.db_path
        
        def setDbPath(self, path: str) -> bool:
        # Implement the method to set the database path
        # You can add logic to validate the path or handle any specific behavior
          self.db_path = path
          return True  # Return True if the operation is successful, or add error handling logic if needed

def parse_csv(csv_file):
    # Function to parse CSV data
    df = pd.read_csv(csv_file)
    print("Processing CSV Data:")
    for index, row in df.iterrows():
        print(f'CSV Row {index + 1}:')
        for column, value in row.items():
            print(f'{column}: {value}')

def parse_json(json_file):
    # Function to parse JSON data and extract authors, venues, references, and publishers
    extracted_data = {"Authors": [], "Venues": [], "References": [], "Publishers": []}  # Initialize lists to store extracted data

    with open(json_file, 'r') as json_data:
        data = json.load(json_data)

    print("Processing JSON Data:")


    # Handle the "authors" section if it exists
    if "authors" in data:
        authors_data = data["authors"]
        for doi, authors_list in authors_data.items():
            for author_info in authors_list:
                if isinstance(author_info, dict):
                    extracted_author = {
                        "DOI": doi,
                        "family": author_info.get("family", ""),
                        "given": author_info.get("given", ""),
                        "orcid": author_info.get("orcid", ""),
                    }
                    extracted_data["Authors"].append(extracted_author)

    # Handle the "venues_id" section if it exists
    if "venues_id" in data:
        venues_data = data["venues_id"]
        for doi, identifiers_list in venues_data.items():
            for identifier in identifiers_list:
                if isinstance(identifier, dict):
                    # Depending on the actual structure of the "venues_id" data, extract "issn" and "isbn"
                    issn = identifier.get("issn", "")
                    isbn = identifier.get("isbn", "")

                    extracted_venue = {
                        "DOI": doi,
                        "issn": issn,
                        "isbn": isbn,
                    }
                    extracted_data["Venues"].append(extracted_venue)

    # Handle the "references" section if it exists
    if "references" in data:
        references_data = data["references"]
        for doi, references_list in references_data.items():
            for reference_info in references_list:
                if isinstance(reference_info, dict):
                    extracted_reference = {
                        "DOI": doi,
                        "reference_DOI": reference_info.get("reference_DOI", ""),
                        "other_data": reference_info.get("other_data", []),
                    }
                    extracted_data["References"].append(extracted_reference)

    # Handle the "publishers" section if it exists
    if "publishers" in data:
        publishers_data = data["publishers"]
        for publisher_id, publisher_info in publishers_data.items():
            if "crossref" in publisher_info:
                crossref_data = publisher_info["crossref"]
                extracted_publisher = {
                    "publisher_id": publisher_id,
                    "name": crossref_data.get("name", ""),
                }
                extracted_data["Publishers"].append(extracted_publisher)

    return extracted_data

def main():
    json_file = 'data/relational_other_data.json'  # Replace with your JSON file's path
    csv_file = 'data/relational_publications.csv'
    extracted_data = parse_json(json_file)
    parse_csv(csv_file)
    print("Extracted Authors Data:")
    for author in extracted_data["Authors"]:
        print(author)

    print("Extracted Venues Data:")
    for venue in extracted_data["Venues"]:
        print(venue)

    print("Extracted References Data:")
    for reference in extracted_data["References"]:
        print(reference)

    print("Extracted Publishers Data:")
    for publisher in extracted_data["Publishers"]:
        print(publisher)


# Example usage of the functions
if __name__ == "__main__":
    json_file = 'data/relational_other_data.json'  # Replace with your JSON file's path
    csv_file = 'data/relational_publications.csv'
    # Parse JSON data and extract authors, venues, references, and publishers
    extracted_data = parse_json(json_file)
    # Parse CSV data
    parse_csv(csv_file)
    # Now, extracted_data contains lists of author, venue, reference, and publisher dictionaries, ready for insertion into the respective tables.
    # You can further process, map, or save this data as needed.
    print("Extracted Authors Data:")
    for author in extracted_data["Authors"]:
        print(author)

    print("Extracted Venues Data:")
    for venue in extracted_data["Venues"]:
        print(venue)

    print("Extracted References Data:")
    for reference in extracted_data["References"]:
        print(reference)

    print("Extracted Publishers Data:")
    for publisher in extracted_data["Publishers"]:
        print(publisher)


if __name__ == "__main":
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

    # Uploading data
    data_path = "/data"  # Path to the data file, is it correct?
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
# Close the database connection
    data_processor.closeConnection()
