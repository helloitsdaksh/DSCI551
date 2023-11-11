import csv
import json


def csv_to_json(csv_file_path, json_file_path):
    # Open the CSV file
    with open(csv_file_path, 'r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Convert CSV to a list of dictionaries
        data = [row for row in csv_reader]

    # Open the JSON file and write the data
    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)


# Example usage:
csv_file_path = '/Users/kohtaasakura/Downloads/soccer/teams.csv'
json_file_path = '../data/soccer_teams.json'
csv_to_json(csv_file_path, json_file_path)