"""
Implementing CRUD Operations
For now, I assume that the data is stored in a single Json file
I will modify the functions to iterate over multiple files later
"""


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


def match_nested_condition(item: dict, condition: dict) -> bool:
    """
    Check if a condition matches in a JSON object in a nested manner
    :param item: The JSON object to be checked
    :param condition: The condition to be checked
    :return: True if the condition matches, False otherwise
    """
    for key, value in condition.items():
        if isinstance(value, dict):
            if key not in item or not match_nested_condition(item[key], value):
                return False
        elif item.get(key) != value:
            return False
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
    for item in data:
        if match_nested_condition(item, json.loads(condition)):
            data.remove(item)
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            print('Deletion successful.')
            return True

    print('Item matching condition not found.')
    return False


def delete_many(database: str, collection: str, condition: str) -> bool:
    """
    Deletes multiple items that match the given condition from a collection in a JSON database file.
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

    # Find the items that match the condition
    to_keep = [item for item in data if not match_nested_condition(item, json.loads(condition))]

    if len(data) == len(to_keep):
        print('No items matching the condition found.')
        return False
    else:
        with open(file_name, 'w') as file:
            json.dump(to_keep, file, indent=2)
        print('Deletion successful.')
        return True


def update_nested_item(item: dict, condition: dict, new_data: dict) -> None:
    """
    Updates a nested item in a JSON object
    :param item:
    :param condition:
    :param new_data:
    :return:
    """
    for key, value in condition.items():
        if isinstance(value, dict) and key in item:
            update_nested_item(item[key], value, new_data)
        else:
            for new_key, new_value in new_data.items():
                item[new_key] = new_value


def update_one(database: str, collection: str, condition: str, new_data: str) -> bool:
    """
    Updates a first itme that matches the given condition in a collection in a JSON database file.
    :param database:
    :param collection:
    :param condition:
    :return: True if the update was successful, False otherwise.
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return False

    for item in data:
        if match_nested_condition(item, json.loads(condition)):
            update_nested_item(item, json.loads(condition), json.loads(new_data))
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            return True

    print('Item matching condition not found.')
    return False


def update_many(database: str, collection: str, condition: str, new_data: str) -> bool:
    """
    Updates multiple items that match the given condition in a collection in a JSON database file.
    :param database:
    :param collection:
    :param condition:
    :param new_data:
    :return: True if the update was successful, False otherwise.
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return False

    updated = False
    for item in data:
        if match_nested_condition(item, json.loads(condition)):
            update_nested_item(item, json.loads(condition), json.loads(new_data))
            updated = True

    if not updated:
        print('No items matching the condition found.')
        return False
    else:
        with open(file_name, 'w') as file:
            json.dump(data, file, indent=2)
        print('Update successful.')
        return True


if __name__ == '__main__':
    database = 'sample'
    collection = 'test'
    # new_data = '{"key1": {"key6": "value6"}, "key2": "value2"}'
    # insert_one(database, collection, new_data)
    # l_data = '[{"key3": "value3", "key4": "value4"},{"key3": "value3", "key4": "value4"}]'
    # insert_many(database, collection, l_data)
    condition = '{"key3": "value3"}'
    # delete_one(database, collection, condition)
    # delete_many(database, collection, condition)
    update_data = '{"key5": "value6"}'
    # update_one(database, collection, condition, update_data)
    update_many(database, collection, condition, update_data)