import json
import os


def insert_one(database: str, collection: str, new_data: str) -> bool:
    """
    Inserts a single item into a collection in a JSON database file.
    :param database: The name of the database.
    :param collection: The name of the collection within the database.
    :param new_data: The data to be inserted as a JSON object.
    :return: True if the insertion was successful, False otherwise.
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        json_data = json.loads(new_data)
    except json.decoder.JSONDecodeError:
        print('Insertion failed. Invalid JSON.')
        return False

    try:  # should check if the collection exists first
        # Load the existing data from the JSON file
        with open(file_name, 'r') as file:
            collection_content = json.load(file)
    except FileNotFoundError:
        # If the database file doesn't exist, create an empty one
        collection_content = []

    # Append the new data to the collection
    collection_content.append(json_data)

    # Write the updated data back to the JSON file
    with open(file_name, 'w') as file:
        json.dump(collection_content, file, indent=2)
    print('Insertion successful.')
    return True


def insert_many(database: str, collection: str, new_data: str) -> bool:
    """
    Inserts multiple items into a collection in a JSON database file.
    :param database: The name of the database.
    :param collection: The name of the collection within the database.
    :param new_data: The data to be inserted as a JSON object.
    :return: True if the insertion was successful, False otherwise.
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        json_data = json.loads(new_data)
    except json.decoder.JSONDecodeError:
        print('Insertion failed. Invalid JSON.')
        return False

    try:
        # Load the existing data from the JSON file
        with open(file_name, 'r') as file:
            collection_content = json.load(file)
    except FileNotFoundError:
        # If the database file doesn't exist, create an empty one
        collection_content = []

    # Append the new data to the collection
    collection_content.extend(json_data)

    # Write the updated data back to the JSON file
    with open(file_name, 'w') as file:
        json.dump(collection_content, file, indent=2)
    print('Insertion successful.')
    return True


def delete_one(database: str, collection: str, condition: str) -> bool:
    """
    Deletes a first itme that matches the given condition from a collection in a JSON database file.
    :param database: The name of the database.
    :param collection: The name of the collection within the database.
    :param condition: The data to be inserted as a JSON object.
    :return: True if the deletion was successful, False otherwise.
    """

    file_name = f'../data/{database}_{collection}.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return False

    # Find the first item that matches the condition
    deleted = False
    for item in data:
        if all(item.get(key) == value for key, value in json.loads(condition).items()):
            data.remove(item)
            deleted = True
            break

    if not deleted:
        print('Item matching condition not found.')
        return False
    else:
        with open(file_name, 'w') as file:
            json.dump(data, file, indent=2)
        print('Deletion successful.')
        return True


def delete_many(database: str, collection: str, condition: str) -> bool:
    """
    Deletes multiple items that match the given condition from a collection in a JSON database file.
    :param database: The name of the database.
    :param collection: The name of the collection within the database.
    :param condition: The data to be inserted as a JSON object.
    :return:
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return False

    # Find the items that match the condition
    deleted = False
    to_keep = data.copy()
    for item in data:
        if all(item.get(key) == value for key, value in json.loads(condition).items()):
            to_keep.remove(item)
            deleted = True

    if not deleted:
        print('Item matching condition not found.')
        return False
    else:
        with open(file_name, 'w') as file:
            json.dump(to_keep, file, indent=2)
        print('Deletion successful.')
        return True


if __name__ == '__main__':
    database = 'sample'
    collection = 'test'
    # new_data = '{"key1": "value1", "key2": "value2"}'
    # insert_one(database, collection, new_data)
    # l_data = '[{"key3": "value3", "key4": "value4"},{"key3": "value3", "key4": "value4"}]'
    # insert_many(database, collection, l_data)
    condition = '{"key1": "value1"}'
    delete_one(database, collection, condition)
    # delete_many(database, collection, condition)