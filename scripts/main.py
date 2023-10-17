import sqlite3
import json
import csv

# Function to insert data into a table
def insert_data(cursor, table_name, data):
    query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({', '.join(['?'] * len(data))})"
    cursor.execute(query, tuple(data.values()))

# Function to insert data from a CSV file
def insert_data_from_csv(cursor, table_name, csv_file):
    with open(csv_file, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            data_to_insert = {key: row[key] for key in row.keys()}
            try:
                insert_data(cursor, table_name, data_to_insert)
            except sqlite3.Error as e:
                print(f"Error inserting data into {table_name} from CSV: {e}")

def main():
    try:
        # Open the database connection
        conn = sqlite3.connect('your_database.db')
        cursor = conn.cursor()

        # Enable foreign key constraints if needed
        cursor.execute("PRAGMA foreign_keys = ON")

        # Load your JSON data
        with open('your_json_data.json', 'r') as json_file:
            data = json.load(json_file)

        # Define the tables and corresponding columns for JSON data
        json_tables = {
            "Identifiable_Entity": ["id"],
            "Another_JSON_Table": ["column1", "column2"],
            # Add more tables and their corresponding columns for JSON as needed
        }

        # Define the tables and corresponding columns for CSV data
        csv_tables = {
            "CSV_Table1": "csv_file1.csv",
            "CSV_Table2": "csv_file2.csv",
            # Add more tables and their corresponding CSV files as needed
        }

        # Insert data from JSON
        for table_name, columns in json_tables.items():
            for item in data.get(table_name, []):
                data_to_insert = {column: item.get(column) for column in columns}
                try:
                    insert_data(cursor, table_name, data_to_insert)
                except sqlite3.Error as e:
                    print(f"Error inserting data into {table_name}: {e}")

        # Insert data from CSV
        for table_name, csv_file in csv_tables.items():
            insert_data_from_csv(cursor, table_name, csv_file)

        # Commit the changes
        conn.commit()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        conn.rollback()  # Rollback changes in case of an error

    finally:
        conn.close()

if __name__ == "__main__":
    main()
