import sqlite3
import pandas as pd
import json

from populate import insert_data

# Function to insert data into a table
def insert_data(cursor, table_name, data):
    placeholders = ', '.join(['?'] * len(data))
    query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({placeholders})"
    cursor.execute(query, list(data.values()))

def main():
    try:
        # Open the database connection
        conn = sqlite3.connect('pabloale.db')  # Use the correct database file name
        cursor = conn.cursor()

        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON")

        # Define the tables and their corresponding data sources
        table_sources = {
            "Publisher": "relational_publications.csv",
            "Event": "relational_publications.csv",
            "Publication": "relational_publications.csv",
            "Identifiable_Entity": "relational_other_data.json",
            "Person": "relational_other_data.json",
            "Venue": "relational_other_data.json",
            "Organization": "relational_other_data.json",
            "Book_Chapter": "relational_other_data.json",
            "Journal_Article": "relational_other_data.json",
            "ProceedingsPaper": "relational_other_data.json",
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
        conn.close()

if __name__ == "__main__":
    main()
