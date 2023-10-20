import pandas as pd
import json

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
    extracted_data = []  # List to store the extracted dictionaries
    with open(json_file, 'r') as json_data:
        data = json.load(json_data)

    print("Processing JSON Data:")
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                # Extract the entire dictionary
                extracted_data.append(item)

                # Optionally, you can print the extracted dictionary
                print('Extracted Dictionary:')
                for key, value in item.items():
                    print(f'{key}: {value}')
            else:
                print("Non-dictionary item found in JSON data.")
    else:
        print("JSON data format not supported.")

    return extracted_data  # Return the extracted dictionaries as a list

# Example usage of the functions
if __name__ == "__main__":
    csv_file = 'data/relational_publications.csv'
    json_file = 'data/relational_other_data.json'

    # Parse CSV data
    parse_csv(csv_file)

    # Parse JSON data
    extracted_data = parse_json(json_file)

    # Now, extracted_data contains a list of dictionaries extracted from the JSON file
    # You can further process, map, or save this data as needed.
