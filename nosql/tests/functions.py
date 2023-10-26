import json
import os


def insert_one(database, collection, new_data):
    """
    Inserts a single document into a collection in a JSON database file.
    :param database: The name of the JSON database file.
    :param collection: The name of the collection within the database.
    :param new_data: The data to be inserted as a Python dictionary.
    :return: True if the insertion was successful, False otherwise.
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        # Load the existing data from the JSON file
        with open(file_name, 'r') as file:
            collection_content = json.load(file)
    except FileNotFoundError:
        # If the database file doesn't exist, create an empty one
        collection_content = []

    # Append the new data to the collection
    collection_content.append(json.loads(new_data))

    # Write the updated data back to the JSON file
    with open(file_name, 'w') as file:
        json.dump(collection_content, file, indent=2)
    return True


if __name__ == '__main__':
    database = 'sample'
    collection = 'test'
    new_data = '{"key1": "value1", "key2": "value2"}'
    insert_one(database, collection, new_data)