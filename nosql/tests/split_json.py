import json
import os


def write_json_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=2)


def get_json_size(data):
    return len(json.dumps(data, indent=2))


def get_next_file_number(base_file_name):
    # Get a list of files that match the base file path
    matching_files = [file for file in os.listdir("../data/.") if file.startswith(base_file_name)]

    # Return the count of matching files plus 1
    return len(matching_files)


def split_json_by_size(base_file_name, input_json, max_size):
    # Convert input JSON string to a Python object (assuming it's an array)
    with open(input_json, 'r') as file:
        json_data = json.load(file)

    current_size = 0
    current_data = []

    for item in json_data:
        # Calculate the size of the current item
        item_size = get_json_size(item)

        # Check if adding the current item exceeds the maximum size
        if current_size + item_size > max_size:
            # If it does, write the current data to a new file
            file_number = get_next_file_number(base_file_name)
            output_filename = f'../data/{base_file_name}_{file_number}.json'
            write_json_to_file(current_data, output_filename)

            # Reset current data and size
            current_data = []
            current_size = 0

        # Add the current item to the current data
        current_data.append(item)
        current_size += item_size

    # Write any remaining data to a new file
    if current_data:
        file_number = get_next_file_number(base_file_name)
        output_filename = f'../data/{base_file_name}_{file_number}.json'
        write_json_to_file(current_data, output_filename)


if __name__ == '__main__':
    split_json_by_size('basketball_teams', '../data/basketball_teams.json', 10000000)