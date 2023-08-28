import sqlparse
#  parse and process the SQL statements 
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
