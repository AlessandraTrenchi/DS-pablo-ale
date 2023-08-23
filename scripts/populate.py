import csv #python libraries
import json
import sqlite3 #to interact with the SQLite database

# Open the database connection
conn = sqlite3.connect('database.db')
cursor = conn.cursor() # create a cursor object

def open_database_connection(db_path):
    try:
        conn = sqlite3.connect(db_path)
        print("Database connection opened")
        return conn
    except sqlite3.Error as e:
        print(f"Error opening database connection: {e}")
        return None

def insert_data_into_database(connection, data):
    query = f"INSERT INTO table_name (column1, column2, column3) VALUES (?, ?, ?)"
    connection.execute(query, data)

def close_database_connection(connection):
    connection.close()