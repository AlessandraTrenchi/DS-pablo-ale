import os
import sqlite3
import json
import csv
import pandas as pd

class RelationalDataProcessor:
    def __init__(self):
        self.db_path = None

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

class RelationalProcessor(RelationalDataProcessor):
    def getDbPath(self):
        return self.db_path

    def setDbPath(self, db_path):
        if os.path.exists(db_path):
            self.db_path = db_path
            return True
        else:
            print(f"Database path '{db_path}' does not exist.")
            return False

    def insert_data_from_csv(self, table_name, csv_file):
        if not self.db_path:
            print("Database path is not set. Please set the database path first.")
            return False

        if not os.path exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return False

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            with open(csv_file, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    data_to_insert = {key: row[key] for key in row.keys()}
                    placeholders = ', '.join(['?'] * len(data_to_insert))
                    query = f"INSERT INTO {table_name} ({', '.join(data_to_insert.keys())}) VALUES ({placeholders})"
                    cursor.execute(query, list(data_to_insert.values()))
            conn.commit()
            conn.close()
            print(f"Data from {csv_file} inserted into {table_name}")
            return True
        except Exception as e:
            print(f"Error inserting data from CSV: {str(e}")
            return False

    def insert_data_from_json(self, table_name, json_file):
        if not self.db_path:
            print("Database path is not set. Please set the database path first.")
            return False

        if not os.path.exists(json_file):
            print(f"JSON file not found: {json_file}")
            return False

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            with open(json_file, 'r') as json_file:
                data = json.load(json_file)

                for item in data:
                    placeholders = ', '.join(['?'] * len(item))
                    query = f"INSERT INTO {table_name} ({', '.join(item.keys())}) VALUES ({placeholders})"
                    cursor.execute(query, list(item.values()))

            conn.commit()
            conn.close()
            print(f"Data from {json_file} inserted into {table_name}")
            return True
        except Exception as e:
            print(f"Error inserting data from JSON: {str(e)}")
            return False

# Example usage:
if __name__ == "__main__":
    rp = RelationalProcessor()

    # Setting the database path
    success = rp.setDbPath("./pabloale.db")
    if success:
        print("Database path set successfully")

    # Uploading data
    data_path = "/data/data_to_upload.txt"  # Path to the data file
    upload_success = rp.uploadData(data_path)
    if upload_success:
        print("Data upload successful")
    else:
        print("Data upload failed")

    # Retrieving the database path
    db_path = rp.getDbPath()
    print("Database path:", db_path)

    # Insert data from CSV
    csv_file = "/data/relational_publications.csv"
    insert_success_csv = rp.insert_data_from_csv("YourTableNameCSV", csv_file)
    if insert_success_csv:
        print("Data insertion from CSV successful")
    else:
        print("Data insertion from CSV failed")

    # Insert data from JSON
    json_file = "/data/relational_other_data.json"
    insert_success_json = rp.insert_data_from_json("YourTableNameJSON", json_file)
    if insert_success_json:
        print("Data insertion from JSON successful")
    else:
        print("Data insertion from JSON failed")
