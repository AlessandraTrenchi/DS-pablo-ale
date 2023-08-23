# script to load and parse RDF data using the rdflib library.
# Import the rdflib library
import sqlparse

def parse_sql_file(file_path):
    with open(file_path, 'r') as sql_file:
        sql_content = sql_file.read()
        
        # Tokenize the SQL content
        statements = sqlparse.split(sql_content)
        
        # Process each SQL statement
        for statement in statements:
            parsed = sqlparse.parse(statement)
            
            # Process the parsed tokens (columns, tables, constraints, etc.)
            for token in parsed[0]:
                # Your processing logic here
                print(token)

if __name__ == "__main__":
    sql_file_path = "database.sql"
    parse_sql_file(sql_file_path)

#if you run python parser.py in the command line, it will parse the RDF data in data.rdf and print the triples