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

# Function to parse JSON data and extract authors, venues, references, and publishers
def parse_json(json_file):
    extracted_data = {"Authors": [], "Venues": [], "References": [], "Publishers": []}  # Initialize lists to store extracted data

    with open(json_file, 'r') as json_data:
        data = json.load(json_data)

    print("Processing JSON Data:")

    # Handle the "authors" section if it exists
    if "authors" in data:
        authors_data = data["authors"]
        for doi, authors_list in authors_data.items():
            for author_info in authors_list:
                if isinstance(author_info, dict):
                    extracted_author = {
                        "DOI": doi,
                        "family": author_info.get("family", ""),
                        "given": author_info.get("given", ""),
                        "orcid": author_info.get("orcid", ""),
                    }
                    extracted_data["Authors"].append(extracted_author)

    # Handle the "venues_id" section if it exists
    if "venues_id" in data:
        venues_data = data["venues_id"]
        for doi, identifiers_list in venues_data.items():
            for identifier in identifiers_list:
                if isinstance(identifier, dict):
                    # Depending on the actual structure of the "venues_id" data, extract "issn" and "isbn"
                    issn = identifier.get("issn", "")
                    isbn = identifier.get("isbn", "")

                    extracted_venue = {
                        "DOI": doi,
                        "issn": issn,
                        "isbn": isbn,
                    }
                    extracted_data["Venues"].append(extracted_venue)

    # Handle the "references" section if it exists
    if "references" in data:
        references_data = data["references"]
        for doi, references_list in references_data.items():
            for reference_info in references_list:
                if isinstance(reference_info, dict):
                    extracted_reference = {
                        "DOI": doi,
                        "reference_DOI": reference_info.get("reference_DOI", ""),
                        "other_data": reference_info.get("other_data", []),
                    }
                    extracted_data["References"].append(extracted_reference)

    # Handle the "publishers" section if it exists
    if "publishers" in data:
        publishers_data = data["publishers"]
        for publisher_id, publisher_info in publishers_data.items():
            if "crossref" in publisher_info:
                crossref_data = publisher_info["crossref"]
                extracted_publisher = {
                    "publisher_id": publisher_id,
                    "name": crossref_data.get("name", ""),
                }
                extracted_data["Publishers"].append(extracted_publisher)

    return extracted_data

# Example usage of the functions
if __name__ == "__main__":
    json_file = 'data/relational_other_data.json'  # Replace with your JSON file's path
    csv_file = 'data/relational_publications.csv'
    # Parse JSON data and extract authors, venues, references, and publishers
    extracted_data = parse_json(json_file)
     # Parse CSV data
    parse_csv(csv_file)
    # Now, extracted_data contains lists of author, venue, reference, and publisher dictionaries, ready for insertion into the respective tables.
    # You can further process, map, or save this data as needed.
    print("Extracted Authors Data:")
    for author in extracted_data["Authors"]:
        print(author)

    print("Extracted Venues Data:")
    for venue in extracted_data["Venues"]:
        print(venue)

    print("Extracted References Data:")
    for reference in extracted_data["References"]:
        print(reference)

    print("Extracted Publishers Data:")
    for publisher in extracted_data["Publishers"]:
        print(publisher)
