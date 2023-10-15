import pandas as pd
import pymysql 

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('relational_publications.csv')

# Establish a database connection
connection = pymysql.connect(host='your_host', user='your_user', password='your_password', database='pabloale')
cursor = connection.cursor()

# Iterate through each row in the DataFrame and insert data
for index, row in df.iterrows():
    id = row['id']
    title = row['title']
    type = row['type']
    publication_year = row['publication_year']
    issue = row['issue']
    volume = row['volume']
    chapter = row['chapter']
    publication_venue = row['publication_venue']
    venue_type = row['venue_type']
    publisher = row['publisher']
    event = row['event']

    # Insert data into the respective tables based on your schema
    # Use SQL INSERT statements or ORM methods to perform the insertion
    # For example:
    # Insert into Publisher table
    cursor.execute("INSERT INTO Publisher (id, name) VALUES (%s, %s)", (id, publisher))

    # Insert into Publication table
    cursor.execute("INSERT INTO Publication (id, title, type, publication_year, issue, volume, chapter, publication_venue, venue_type, publisher_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (id, title, type, publication_year, issue, volume, chapter, publication_venue, venue_type, id))
    
    # Insert into Event table
    cursor.execute("INSERT INTO Event (event_detail) VALUES (%s)", (event,))

# Commit the changes to the database if you're using a transaction
connection.commit()

# Close the database connection
connection.close()
