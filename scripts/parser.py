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
    extracted_data = {}  # Dictionary to store extracted data
    with open(json_file, 'r') as json_data:
        data = json.load(json_data)
    
    print("Processing JSON Data:")
    
    # Handle "authors" section
    if "authors" in data:
        extracted_data["authors"] = {}
        for doi, authors_list in data["authors"].items():
            extracted_data["authors"][doi] = []
            for author_info in authors_list:
                if isinstance(author_info, dict):
                    extracted_data["authors"][doi].append(author_info)
    
    # Handle "venues_id" section
    if "venues_id" in data:
        extracted_data["venues_id"] = {}
        for doi, venues_list in data["venues_id"].items():
            extracted_data["venues_id"][doi] = venues_list
    
    # Handle "publishers" section
    if "publishers" in data:
        extracted_data["publishers"] = {}
        for publisher_id, publisher_info in data["publishers"].items():
            extracted_data["publishers"][publisher_id] = publisher_info
    
    # Handle "references" section
    if "references" in data:
        extracted_data["references"] = {}
        for doi, references_list in data["references"].items():
            extracted_data["references"][doi] = references_list
    
    return extracted_data  # Return the extracted data as a dictionary

# Example usage of the function
if __name__ == "__main__":
    json_file = 'your_json_file.json'  # Replace with your JSON file's path
    extracted_data = parse_json(json_file)


# Example usage of the functions
if __name__ == "__main__":
    csv_file = 'data/relational_publications.csv'
    json_file = 'data/relational_other_data.json'

    # Parse CSV data
    parse_csv(csv_file)

    # Parse JSON data
    extracted_data = parse_json(json_file)

    # Now, extracted_data contains a dictionary with nested dictionaries extracted from the JSON file
    # You can further process, map, or save this data as needed.
