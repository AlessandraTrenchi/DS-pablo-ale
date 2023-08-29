import sqlite3 # for working with the sqlite database
import csv
import json


def insert_publisher(cursor, id, name): 
    query = "INSERT INTO Publishers (id, name) VALUES (?, ?)" #query to insert data into the Publishers table
    cursor.execute(query, (id, name)) #execute the query with the provided data

def insert_publication(cursor, id, doi):
    query = "INSERT INTO Publications (id, doi) VALUES (?, ?)"
    cursor.execute(query, (id, doi))

def insert_reference(cursor, source_doi, target_doi):
    query = "INSERT INTO References (source_doi, target_doi) VALUES (?, ?)"
    cursor.execute(query, (source_doi, target_doi))

# Open the database connection
conn = sqlite3.connect('ds-pablo-ale.db')
cursor = conn.cursor() #creating a cursor object

# Populate Publishers from CSV
with open('publishers.csv', 'r') as csv_file: # publishers.csv', creates a CSV reader, and iterates over each row in the CSV file
    csv_reader = csv.reader(csv_file) #  skips the header row using next(csv_reader)
    next(csv_reader)  # Skip header row

    for row in csv_reader:
        insert_publisher(cursor, *row)

# Populate Publications from JSON
with open('publications.json', 'r') as json_file:
    publications_data = json.load(json_file)

    for publication in publications_data:
        insert_publication(cursor, publication['id'], publication['doi'])

# Populate References from JSON
with open('references.json', 'r') as json_file:
    references_data = json.load(json_file)

    for reference in references_data:
        insert_reference(cursor, reference['source_doi'], reference['target_doi'])

# Commit the changes and close the connection
conn.commit()
conn.close()
