import sqlite3
import json
import csv

# Function to insert data into a table
def insert_data(cursor, table_name, data):
    placeholders = ', '.join(['?'] * len(data))
    query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({placeholders})"
    cursor.execute(query, list(data.values()))

def insert_data_from_csv(cursor, table_name, csv_file):
    try:
        with open(csv_file, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                data_to_insert = {key: row[key] for key in row.keys()}
                insert_data(cursor, table_name, data_to_insert)
    except Exception as e:
        print(f"Error reading CSV file '{csv_file}': {e}")

def insert_data_from_json(cursor, table_name, json_data):
    for item in json_data:
        try:
            insert_data(cursor, table_name, item)
        except sqlite3.Error as e:
            print(f"Error inserting data into {table_name}: {e}")

# You can add similar error handling for other data sources as needed
