import pandas as pd
import json

csv_file = 'data/relational_publications.csv'
json_file = 'data/relational_other_data.json'

# Function to parse CSV data
def parse_csv(csv_file):
    df = pd.read_csv(csv_file)
    print("Processing CSV Data:")
    for index, row in df.iterrows():
        print(f'CSV Row {index + 1}:')
        for column, value in row.items():
            print(f'{column}: {value}')

# Function to parse JSON data
def parse_json(json_file):
    with open(json_file, 'r') as json_data:
        data = json.load(json_data)
    print("Processing JSON Data:")
    if isinstance(data, list):
        for item in data:
            print('JSON Item:')
            for key, value in item.items():
                print(f'{key}: {value}')
    else:
        print("JSON data format not supported.")

# Example usage of the functions
if __name__ == "__main__": #the code within this block is executed when the script is run as the main program.
    csv_file = 'data/relational_publications.csv'
    json_file = 'data/relational_other_data.json'

    # Parse CSV data
    parse_csv(csv_file)

    # Parse JSON data
    parse_json(json_file)




