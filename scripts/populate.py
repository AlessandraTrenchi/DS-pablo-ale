import sqlite3
import csv
import json

def insert_publisher(cursor, id, name): 
    query = "INSERT INTO Publishers (id, name) VALUES (?, ?)"
    cursor.execute(query, (id, name))

# Function to insert data into the Event table
def insert_event(cursor, id, event_detail):
    query = "INSERT INTO Event (event_detail) VALUES (?)"
    cursor.execute(query, (id, event_detail))

def insert_publication(cursor, id, title, type, publication_year, issue, volume, chapter, publication_venue, venue_type, publisher_id, event_id):
    query = "INSERT INTO Publication (id, title, type, publication_year, issue, volume, chapter, publication_venue, venue_type, publisher_id, event_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.execute(query, (id, title, type, publication_year, issue, volume, chapter, publication_venue, venue_type, publisher_id, event_id))

# Function to insert data into the IdentifiableEntity table
def insert_identifiable_entity(cursor, id):
    query = "INSERT INTO IdentifiableEntity (id) VALUES (?)"
    cursor.execute(query, (id,))

def insert_person(cursor, identifiable_entity_id, given_name, family_name):
    query = "INSERT INTO Person (identifiableEntityId, givenName, familyName) VALUES (?, ?, ?)"
    cursor.execute(query, (identifiable_entity_id, given_name, family_name))

def insert_venue(cursor, identifiable_entity_id, title, venue_type):
    query = "INSERT INTO Venue (IdentifiableEntityId, title, type) VALUES (?, ?, ?)"
    cursor.execute(query, (identifiable_entity_id, title, venue_type))

def insert_organization(cursor, identifiable_entity_id, name):
    query = "INSERT INTO Organization (IdentifiableEntityId, name) VALUES (?, ?)"
    cursor.execute(query, (identifiable_entity_id, name))

def insert_publication_venue(cursor, publication_id, venue_id):
    query = "INSERT INTO PublicationVenue (PublicationId, VenueId) VALUES (?, ?)"
    cursor.execute(query, (publication_id, venue_id))

def insert_citation(cursor, citing_publication_id, cited_publication_id):
    query = "INSERT INTO Citation (CitingPublicationId, CitedPublicationId) VALUES (?, ?)"
    cursor.execute(query, (citing_publication_id, cited_publication_id))

def insert_book_chapter(cursor, publication_id, chapter_number):
    query = "INSERT INTO BookChapter (PublicationId, chapterNumber) VALUES (?, ?)"
    cursor.execute(query, (publication_id, chapter_number))

def insert_journal_article(cursor, publication_id):
    query = "INSERT INTO JournalArticle (PublicationId) VALUES (?)"
    cursor.execute(query, (publication_id,))

def insert_proceedings_paper(cursor, publication_id):
    query = "INSERT INTO ProceedingsPaper (PublicationId) VALUES (?)"
    cursor.execute(query, (publication_id,))

# Function to insert data into the Authors table
def insert_author(cursor, family, given, orcid, publication_id):
    query = "INSERT INTO Authors (family, given, orcid, publication_id) VALUES (?, ?, ?, ?)"
    cursor.execute(query, (family, given, orcid, publication_id))

# Function to insert data into the Publications table
def insert_publication(cursor, id, doi):
    query = "INSERT INTO Publications (id, doi) VALUES (?, ?)"
    cursor.execute(query, (id, doi))

def insert_reference(cursor, source_doi, target_doi):
    query = "INSERT INTO References (source_doi, target_doi) VALUES (?, ?)"
    cursor.execute(query, (source_doi, target_doi))

# Function to insert data into the Publishers table
def insert_publisher(cursor, id, name): 
    query = "INSERT INTO Publishers (id, name) VALUES (?, ?)"
    cursor.execute(query, (id, name))

# Function to insert data into the AuthorPublication table
def insert_author_publication(cursor, author_id, publication_id):
    query = "INSERT INTO AuthorPublication (author_id, publication_id) VALUES (?, ?)"
    cursor.execute(query, (author_id, publication_id))

def insert_data_from_csv(cursor, relational_publications):
    with open(relational_publications, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header row

        for row in csv_reader:
            try:
                insert_publisher(cursor, *row) # Insert data into Publishers table
            except sqlite3.Error as e:
                print(f"Error inserting data from CSV: {e}")

def insert_data_from_json(cursor, relational_other_data, insert_function):
    with open(relational_other_data, 'r') as json_file:
        data = json.load(json_file)
        for item in data:
            try:
                insert_function(cursor, item['id'], item['doi'])
            except sqlite3.Error as e:
                print(f"Error inserting data from JSON: {e}")


try:
    # Open the database connection
    conn = sqlite3.connect('ds-pablo-ale.db')
    cursor = conn.cursor()

    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")

    # Populate Publishers from CSV
    insert_data_from_csv(cursor, 'data/relational_publications.csv')

    # Populate Publications from JSON
    insert_data_from_json(cursor, 'data/relational_other_data.json', insert_publication)

    # Commit the changes
    conn.commit()

except sqlite3.Error as e:
    print(f"SQLite error: {e}")
    conn.rollback()  # Rollback changes in case of error

finally:
    conn.close()
