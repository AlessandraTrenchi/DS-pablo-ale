import sqlite3
import csv
import json

def insert_publisher(cursor, id, name): 
    query = "INSERT INTO Publishers (id, name) VALUES (?, ?)"
    cursor.execute(query, (id, name))

def insert_publication(cursor, id, doi):
    query = "INSERT INTO Publications (id, doi) VALUES (?, ?)"
    cursor.execute(query, (id, doi))

def insert_reference(cursor, source_doi, target_doi):
    query = "INSERT INTO References (source_doi, target_doi) VALUES (?, ?)"
    cursor.execute(query, (source_doi, target_doi))

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
    insert_data_from_csv(cursor, 'publishers.csv')

    # Populate Publications from JSON
    insert_data_from_json(cursor, 'publications.json', insert_publication)

    # Populate References from JSON
    insert_data_from_json(cursor, 'references.json', insert_reference)

    # Commit the changes
    conn.commit()

except sqlite3.Error as e:
    print(f"SQLite error: {e}")
    conn.rollback()  # Rollback changes in case of error

finally:
    conn.close()
