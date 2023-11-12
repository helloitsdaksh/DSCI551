"""
Implementing CRUD Operations
For now, I assume that the data is stored in a single Json file
I will modify the functions to iterate over multiple files later
"""


import json
import os

METADATA_FILE = 'metadata.json'
MAX_FILE_SIZE = 10000000  # 10 MB
# MAX_FILE_SIZE = 1000


def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as file:
            return json.load(file)
    else:
        return {}


def save_metadata(metadata):
    with open(METADATA_FILE, 'w') as file:
        json.dump(metadata, file, indent=2)


def update_metadata(database_name: str, collection_name: str, file_number: int):
    metadata = load_metadata()

    for database in metadata.get('databases', []):
        if database['name'] == database_name:
            for collection in database['collections']:
                if collection['name'] == collection_name:
                    collection['partition_count'] = file_number
                    save_metadata(metadata)
                    return True

            # If the collection doesn't exist, add a new entry
            database['collections'].append({'name': collection_name, 'partition_count': file_number})
            save_metadata(metadata)
            return True


def get_last_file_number(database_name: str, collection_name: str) -> list or None:
    """
    Returns the last file number of a collection in a JSON database file.
    :param database_name: The name of the database.
    :param collection_name: The name of the collection within the database.
    :return: The last file number or None if the collection does not exist.
    """
    metadata = load_metadata()
    try:  # should check if the collection exists first
        for database in metadata['databases']:
            if database['name'] == database_name:
                for collection in database['collections']:
                    if collection['name'] == collection_name:
                        file_number = collection['partition_count']
                        return file_number
    except KeyError:
        return None


def insert_one(database_name: str, collection_name: str, new_data: str) -> bool:
    """
    Inserts a single item into a collection in a JSON database file.
    :param database_name: The name of the database.
    :param collection_name: The name of the collection within the database.
    :param new_data: The data to be inserted as a JSON object.
    :return: True if the insertion was successful, False otherwise.
    """
    try:
        json_data = json.loads(new_data)
    except json.decoder.JSONDecodeError:
        print('Insertion failed. Invalid JSON.')
        return False

    existing_file_number = get_last_file_number(database_name, collection_name)
    if existing_file_number is None:
        existing_data = [json_data]
        file_name = f'../data/{database_name}_{collection_name}_1.json'
        with open(file_name, 'w') as file:
            json.dump(existing_data, file, indent=2)

        update_metadata(database_name, collection_name, 1)

    else:
        with open(f'../data/{database_name}_{collection_name}_{existing_file_number}.json', 'r') as file:
            existing_data = json.load(file)

        if len(existing_data) + len(json_data) >= MAX_FILE_SIZE:
            with open(f'../data/{database_name}_{collection_name}_{existing_file_number+1}.json', 'w') as new_file:
                json.dump([json_data], new_file, indent=2)
            update_metadata(database_name, collection_name, existing_file_number+1)
        else:
            existing_data.append(json_data)
            with open(f'../data/{database_name}_{collection_name}_{existing_file_number}.json', 'w') as file:
                json.dump(existing_data, file, indent=2)

    return True


def insert_many(database_name: str, collection_name: str, new_data: str) -> bool:
    """
    Inserts multiple items into a collection in a JSON database file.
    :param database_name: The name of the database.
    :param collection_name: The name of the collection within the database.
    :param new_data: The data to be inserted as a JSON object.
    :return: True if the insertion was successful, False otherwise.
    """
    try:
        json_data = json.loads(new_data)
    except json.decoder.JSONDecodeError:
        print('Insertion failed. Invalid JSON.')
        return False

    existing_file_number = get_last_file_number(database_name, collection_name)
    if existing_file_number is None:
        existing_data = [json_data]
        file_name = f'../data/{database_name}_{collection_name}_1.json'
        with open(file_name, 'w') as file:
            json.dump(existing_data, file, indent=2)

        update_metadata(database_name, collection_name, 1)

    else:
        with open(f'../data/{database_name}_{collection_name}_{existing_file_number}.json', 'r') as file:
            existing_data = json.load(file)

        if len(existing_data) + len(json_data) >= MAX_FILE_SIZE:
            update_metadata(database_name, collection_name, existing_file_number+1)
            overflow_data = []
            for item in json_data:
                if len(existing_data) + len(item) >= MAX_FILE_SIZE:
                    overflow_data.append(item)
                else:
                    existing_data.append(item)
            with open(f'../data/{database_name}_{collection_name}_{existing_file_number+1}.json', 'w') as file:
                json.dump(overflow_data, file, indent=2)
            with open(f'../data/{database_name}_{collection_name}_{existing_file_number}.json', 'w') as file:
                json.dump(existing_data, file, indent=2)

        else:
            existing_data.extend(json_data)
            with open(f'../data/{database_name}_{collection_name}_{existing_file_number}.json', 'w') as file:
                json.dump(existing_data, file, indent=2)

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


def delete_one(database: str, collection: str, condition: dict) -> bool:
    """
    Deletes the first item that matches the given condition from a collection in a JSON database file.
    :param database: The name of the database.
    :param collection: The name of the collection within the database.
    :param condition: A dictionary specifying the condition for matching items.
    :return: True if the deletion was successful, False otherwise.
    """
    file_name = f'../data/{database}_{collection}.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return False

    dict_condition = json.loads(condition)

    if not dict_condition:  # Check if the condition is empty
        if data:
            item_to_delete = data[0]  # Get the first item in the collection
            data.remove(item_to_delete)
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            print('Deletion successful.')
            return True
        else:
            print('Collection is empty. Nothing to delete.')
            return False

    for item in data:
        if match_nested_condition(item, dict_condition):
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

    dict_condition = json.loads(condition)

    if not dict_condition:  # Check if the condition is empty
        with open(file_name, 'w') as file:
            json.dump([], file, indent=2)  # Empty the file
        print('All data deleted.')
        return True

    # Find the items that match the condition
    to_keep = [item for item in data if not match_nested_condition(item, dict_condition)]

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

    dict_condition = json.loads(condition)
    dict_new_data = json.loads(new_data)

    if not dict_condition:  # Check if the condition is empty
        if data:
            item_to_update = data[0]  # Get the first item in the collection
            # Update the first item
            for key, value in dict_new_data.items():
                item_to_update[key] = value
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            print('Update successful.')
            return True
        else:
            print('Collection is empty. Nothing to update.')
            return False

    for item in data:
        if match_nested_condition(item, dict_condition):
            update_nested_item(item, dict_condition, dict_new_data)
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

    dict_condition = json.loads(condition)
    dict_new_data = json.loads(new_data)

    if not dict_condition:  # Check if the condition is empty
        if data:
            for item in data:
                for key, value in dict_new_data.items():
                    item[key] = value
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            print('Update successful.')
            return True

    updated = False
    for item in data:
        if match_nested_condition(item, dict_condition):
            update_nested_item(item, dict_condition, dict_new_data)
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
    database = 'basketball'
    collection = 'test'
    # new_data = '{"PLAYER_NAME": "John Smith", "TEAM_ID": "0000", "PLAYER_ID": "0000", "SEASON": "2023"}'
    # insert_one(database, collection, new_data)
    l_data = '[{"PLAYER_NAME": "John Smith", "TEAM_ID": "0000", "PLAYER_ID": "0000", "SEASON": "2023"}, {"PLAYER_NAME": "John Smith", "TEAM_ID": "0000", "PLAYER_ID": "0000", "SEASON": "2023"}]'
    insert_many(database, collection, l_data)
    # condition = '{"key3": "value3"}'
    # delete_one(database, collection, condition)
    # delete_many(database, collection, '{}')
    # delete_many(database, collection, condition)
    # update_data = '{"key5": "value6"}'
    # update_many(database, collection, '{}', update_data)
    # update_many(database, collection, condition, update_data)