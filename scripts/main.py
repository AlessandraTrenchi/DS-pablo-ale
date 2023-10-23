import sqlite3
import pandas as pd
import json

from populate import insert_data

def open_database_connection(database_file):
    try:
        # Open the database connection
        conn = sqlite3.connect(database_file)
        conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def close_database_connection(conn):
    if conn:
        try:
            conn.commit()  # Commit changes (if any)
        except sqlite3.Error as e:
            print(f"Error committing changes: {e}")
        
        conn.close()  # Close the database connection
        print("Database connection closed")

# Function to insert data into a table
def insert_data(cursor, table_name, data):
    placeholders = ', '.join(['?'] * len(data))
    query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({placeholders})"
    cursor.execute(query, list(data.values()))

def main():
    try:
        # Open the database connection
        conn = open_database_connection('pabloale.db')  # Use the correct database file name
        cursor = conn.cursor()

        # Define the tables and their corresponding data sources
        table_sources = {
            "Publishers": "data/relational_other_data.json",
            # "Event": "relational_publications.csv",
            "Publication": "relational_publications.csv",
            "Authors": "data/relational_other_data.json", #"Person": "data/relational_other_data.json", #Authors' properties
            #"Venue": "data/relational_publications.csv", #Publication property: title, type 
            "Venues_id" : "data/relational_other_data.json",
           # "Book_Chapter": "relational_other_data.json", #venue_type property
            # "Journal_Article": "relational_other_data.json", #venue_type property
        }

        # Insert data from various sources
        for table_name, source_file in table_sources.items():
            if source_file.endswith(".json"):
                with open(source_file, 'r') as json_file:
                    data = json.load(json_file)
                    for item in data:
                        insert_data(cursor, table_name, item)
            elif source_file.endswith(".csv"):
                df = pd.read_csv(source_file)
                for index, row in df.iterrows():
                    data = {
                        'id': row['id'],
                        'identifiable_entity_id': row['identifiable_entity_id'],
                        'title': row['title'],
                        'type': row['type']
                    }
                    insert_data(cursor, table_name, data)

        # Commit the changes
        conn.commit()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        conn.rollback()  # Rollback changes in case of an error

    finally:
        close_database_connection(conn)

if __name__ == "__main__":
    main()
