import csv #python libraries
import json
import sqlite3 #to interact with the SQLite database

# Open the database connection
conn = sqlite3.connect('database.db')
cursor = conn.cursor() # create a cursor object
