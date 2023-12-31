# Import the sqlite3 module for working with SQLite databases
import sqlite3
from populate import parse_line, insert_data_into_database, close_database_connection
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
# A class is a blueprint for creating objects
# utility classes that you've defined to handle specific tasks related to managing and querying the data

class DataManager: # Base class for common methods to manage database connections
    def __init__(self):
        self.dbPath = ""
        self.endpointUrl = ""

    def setDbPath(self, dbPath):
        self.dbPath = dbPath

    def getDbPath(self):
        return self.dbPath

    def setEndpointUrl(self, endpointUrl):
        self.endpointUrl = endpointUrl

    def getEndpointUrl(self):
        return self.endpointUrl

class RelationalDataProcessor(DataManager): #inerits DataManager
    def uploadData(self, filePath): #method that reads data and inserts it into the relational database
        conn = None  # Initialize the database connection to None
        try:
            conn = open_database_connection(self.dbPath)  # Open a connection to the database
            
            # Read data from the file and insert it into the database
            with open(filePath, 'r') as file:
                for line in file:
                    data = parse_line(line)  # Parse the line into data fields
                    insert_data_into_database(conn, data)  # Insert data into appropriate tables
                    
            print(f"Uploaded data from {filePath} to the relational database")
        except Exception as e:
            print(f"Error uploading data: {e}")
        finally:
            if conn:
                close_database_connection(conn)  # Close the database connection

class TriplestoreDataProcessor(DataManager): #handles interactions with a triplestore
    #triplestore = database optimized to store and query RDF triples
    def uploadData(self, filePath): 
        triples = []  # Store parsed triples here
        
        # Read data from the file and parse it into triples
        #The with statement ensures that the file is properly closed after its suite (indented block) is executed
        with open(filePath, 'r') as file:
            for line in file:
                subject, predicate, obj = line.strip().split(',')  # splits the line into 3 components: subject, predicate, object
                triples.append((subject, predicate, obj)) # Appends a tuple (subject, predicate, obj) to the triples list. This tuple represents a RDF triple.
        
        # Upload the triples to the triplestore
        for triple in triples:
            subject, predicate, obj = triple
            upload_to_triplestore(subject, predicate, obj)
        print(f"Uploaded {len(triples)} triples to the triplestore")

# Class for querying relational data
class RelationalQueryProcessor:
    def getPublicationsPublishedInYear(self, year): # Implement query logic for getting publications by year from relational database
        print(f"Getting publications published in year {year}")

    def getPublicationsByAuthorId(self, authorId):
        print(f"Getting publications by author id {authorId}")

    # ... implement other query methods ...

# Class for querying triplestore data
class TriplestoreQueryProcessor:
    def getMostCitedPublication(self):
        print("Getting most cited publication")

    def getMostCitedVenue(self):
        print("Getting most cited venue")

    # ... implement other query methods ...

# Define the function to open a database connection
def open_database_connection(db_path):
    try:
        conn = sqlite3.connect(db_path)  # Establish a connection to the SQLite database
        print("Database connection opened")
        return conn
    except sqlite3.Error as e:
        print(f"Error opening database connection: {e}")
        return None

# Usage
# ... create instances of data processors and perform data operations ...

# Usage
relational_data_processor = RelationalDataProcessor()
relational_data_processor.setDbPath("path/to/relational.db")
relational_data_processor.uploadData("data/relational_data.csv")

triplestore_data_processor = TriplestoreDataProcessor()
triplestore_data_processor.setEndpointUrl("http://example.com/sparql") #endpointURL url of my SPARQL
triplestore_data_processor.uploadData("data/triplestore_data.json")

relational_query_processor = RelationalQueryProcessor()
relational_query_processor.getPublicationsPublishedInYear(2020)
relational_query_processor.getPublicationsByAuthorId("0000-0001-9857-1511")

triplestore_query_processor = TriplestoreQueryProcessor()
triplestore_query_processor.getMostCitedPublication()
triplestore_query_processor.getMostCitedVenue()

