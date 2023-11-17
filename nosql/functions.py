"""
Implementing CRUD Operations
## update operations do not check the file size
"""


import json
import os

METADATA_FILE = 'metadata.json'
MAX_FILE_SIZE = 4000000  # 4MB
# MAX_FILE_SIZE = 100


def load_metadata():
    """
    Load metadata from a JSON file.
    :return: database metadata
    """
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as file:
            return json.load(file)
    else:
        return {}


def save_metadata(metadata):
    """
    Save metadata to a JSON file.
    :param metadata:
    :return: None
    """
    with open(METADATA_FILE, 'w') as file:
        json.dump(metadata, file, indent=2)


def update_metadata(database_name: str, collection_name: str, file_number: int) -> bool:
    """
    Update the metadata of a collection in a JSON database file.
    e.g. adding new data, deleting data, etc.
    :param database_name:
    :param collection_name:
    :param file_number:
    :return: True if the operation is successful, False otherwise
    """
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
    return False


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


def show_collections(database_name: str):
    """
    Show all collections in a database
    :param database_name:
    :return:
    """
    metadata = load_metadata()
    for database in metadata['databases']:
        if database['name'] == database_name:
            for collection in database['collections']:
                print(collection['name'])


def show_databases():
    """
    Show all databases
    :return:
    """
    metadata = load_metadata()
    for database in metadata['databases']:
        print(database['name'])


def create_collection(database_name: str, collection_name: str):
    """
    Create a new collection in a JSON database file
    :param database_name:
    :param collection_name:
    :return:
    """
    metadata = load_metadata()
    for database in metadata['databases']:
        if database['name'] == database_name:
            database['collections'].append({'name': collection_name, 'partition_count': 1})
            save_metadata(metadata)
            with open(f'data/{database_name}_{collection_name}_1.json', 'w') as file:
                json.dump([], file)
            return True
    return False


def create_database(database_name: str):
    """
    Create a new database
    :param database_name:
    :return:
    """
    metadata = load_metadata()
    metadata['databases'].append({'name': database_name, 'collections': []})
    save_metadata(metadata)
    return True


def drop_collection(database_name: str, collection_name: str):
    """
    Delete a collection in a JSON database file
    :param database_name:
    :param collection_name:
    :return:
    """
    metadata = load_metadata()
    for database in metadata['databases']:
        if database['name'] == database_name:
            for collection in database['collections']:
                if collection['name'] == collection_name:
                    os.remove(f'data/{database_name}_{collection_name}_{collection["partition_count"]}.json')
                    database['collections'].remove(collection)
                    save_metadata(metadata)
                    return True
    return False


def drop_database(database_name: str):
    """
    Delete a database (this actually don't delete the data because I want to keep it)
    :param database_name:
    :return:
    """
    metadata = load_metadata()
    for database in metadata['databases']:
        if database['name'] == database_name:
            metadata['databases'].remove(database)
            save_metadata(metadata)
            return True
    return False


def calculate_data_size(data):
    json_string = json.dumps(data)
    byte_data = json_string.encode('utf-8')
    return len(byte_data)


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
        print('Insertion failed. Collection does not exist.')
        return False

    else:
        with open(f'data/{database_name}_{collection_name}_{existing_file_number}.json', 'r') as file:
            existing_data = json.load(file)
            existing_data_size = calculate_data_size(existing_data)
            new_data_size = calculate_data_size(json_data)

        if existing_data_size + new_data_size >= MAX_FILE_SIZE:
            with open(f'data/{database_name}_{collection_name}_{existing_file_number+1}.json', 'w') as new_file:
                json.dump([json_data], new_file, indent=2)
            update_metadata(database_name, collection_name, existing_file_number+1)
        else:
            existing_data.append(json_data)
            with open(f'data/{database_name}_{collection_name}_{existing_file_number}.json', 'w') as file:
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
        file_name = f'../data/{database_name}_{collection_name}_1.json'
        with open(file_name, 'w') as file:
            json.dump(json_data, file, indent=2)

        update_metadata(database_name, collection_name, 1)

    else:
        with open(f'data/{database_name}_{collection_name}_{existing_file_number}.json', 'r') as file:
            existing_data = json.load(file)
            existing_data_size = calculate_data_size(existing_data)
            new_data_size = calculate_data_size(json_data)

        if existing_data_size + new_data_size >= MAX_FILE_SIZE:
            update_metadata(database_name, collection_name, existing_file_number+1)
            overflow_data = []
            for item in json_data:
                if existing_data_size + calculate_data_size(item) >= MAX_FILE_SIZE:
                    overflow_data.append(item)
                else:
                    existing_data.append(item)
            with open(f'data/{database_name}_{collection_name}_{existing_file_number+1}.json', 'w') as file:
                json.dump(overflow_data, file, indent=2)
            with open(f'data/{database_name}_{collection_name}_{existing_file_number}.json', 'w') as file:
                json.dump(existing_data, file, indent=2)

        else:
            existing_data.extend(json_data)
            with open(f'data/{database_name}_{collection_name}_{existing_file_number}.json', 'w') as file:
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


def delete_one(database_name: str, collection_name: str, condition: str) -> bool:
    """
    Deletes the first item that matches the given condition from a collection in a JSON database file.
    Deletes the first item in collection if condition is empty = {}
    :param database_name: The name of the database.
    :param collection_name: The name of the collection within the database.
    :param condition: A dictionary specifying the condition for matching items.
    :return: True if the deletion was successful, False otherwise.
    """
    try:
        dict_condition = json.loads(condition)
    except json.decoder.JSONDecodeError:
        print('Deletion failed. Invalid JSON.')
        return False

    existing_file_number = get_last_file_number(database_name, collection_name)

    if not dict_condition: # if condition is empty, delete first item
        file_name = f'data/{database_name}_{collection_name}_1.json'
        with open(file_name, 'r+') as file:
            data = json.load(file)
            if len(data) > 0:
                data.pop(0)
                file.seek(0)
                json.dump(data, file, indent=2)
                file.truncate()
                print('Deletion successful.')
            else:
                print('Collection is empty. Nothing to delete.')
                return False

    else:
        for i in range(1, existing_file_number+1):
            file_name = f'data/{database_name}_{collection_name}_{i}.json'
            try:
                with open(file_name, 'r') as file:
                    data = json.load(file)
            except FileNotFoundError:
                print(f'File {file_name} not found.')
                return False

            if len(data) > 0:
                for item in data:
                    if match_nested_condition(item, dict_condition):
                        data.remove(item)
                        with open(file_name, 'w') as file:
                            json.dump(data, file, indent=2)
                        print('Deletion successful.')
                        return True
            else:
                print('Collection is empty. Nothing to delete.')
                return False

    print('Item matching condition not found.')
    return False


def delete_many(database_name: str, collection_name: str, condition: str) -> bool:
    """
    Deletes multiple items that match the given condition from a collection in a JSON database file.
    Deletes all the items if condition is empty = {}
    ## does not consider a situation where the condition matches all the items in the collection and delete all
    :param database_name: The name of the database.
    :param collection_name: The name of the collection within the database.
    :param condition: The data to be inserted as a JSON object.
    :return: True if the deletion was successful, False otherwise.
    """
    try:
        dict_condition = json.loads(condition)
    except json.decoder.JSONDecodeError:
        print('Deletion failed. Invalid JSON.')
        return False

    existing_file_number = get_last_file_number(database_name, collection_name)

    deleted = False

    for i in range(1, existing_file_number + 1):
        file_name = f'data/{database_name}_{collection_name}_{i}.json'
        try:
            with open(file_name, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f'File {file_name} not found.')
            return False

        if not dict_condition:  # if the condition is empty, delete all items
            if i > 1:
                os.remove(file_name)
            else:
                with open(file_name, 'w') as file:
                    json.dump([], file, indent=2)  # Empty the file
                update_metadata(database_name, collection_name, 1)
            deleted = True

        else:
            to_keep = [item for item in data if not match_nested_condition(item, dict_condition)]
            if len(data) == len(to_keep):
                continue
            else:
                with open(file_name, 'w') as file:
                    json.dump(to_keep, file, indent=2)
                deleted = True

    if deleted:
        print('Deletion successful.')
        return True
    else:
        print('Item matching condition not found.')
        return False


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


def update_one(database_name: str, collection_name: str, condition: str, new_data: str) -> bool:
    """
    Updates a first itme that matches the given condition in a collection in a JSON database file.
    Update the first item if the condition is empty = {}
    :param database_name:
    :param collection_name:
    :param condition:
    :param new_data:
    :return: True if the update was successful, False otherwise.
    """
    try:
        dict_condition = json.loads(condition)
        dict_new_data = json.loads(new_data)
    except json.decoder.JSONDecodeError:
        print('Update failed. Invalid JSON.')
        return False

    existing_file_number = get_last_file_number(database_name, collection_name)

    if not dict_condition:  # if condition is empty, update the first item
        file_name = f'data/{database_name}_{collection_name}_1.json'
        with open(file_name, 'r+') as file:
            data = json.load(file)
            if len(data) > 0:
                item_to_update = data[0]  # Get the first item in the collection
                for key, value in dict_new_data.items():
                    item_to_update[key] = value
                file.seek(0)  # Move the file cursor to the beginning before writing
                json.dump(data, file, indent=2)
                file.truncate()  # Truncate the file in case the new content is shorter
                print('Update successful.')
                return True

    for i in range(1, existing_file_number+1):
        file_name = f'data/{database_name}_{collection_name}_{i}.json'
        try:
            with open(file_name, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f'File {file_name} not found.')
            return False

        if len(data) > 0:
            for item in data:
                if match_nested_condition(item, dict_condition):
                    update_nested_item(item, dict_condition, dict_new_data)
                    with open(file_name, 'w') as file:
                        json.dump(data, file, indent=2)
                    print('Update successful.')
                    return True

        else:
            print('Collection is empty. Nothing to update.')
            return False

    print('Item matching condition not found.')
    return False


def update_many(database_name: str, collection_name: str, condition: str, new_data: str) -> bool:
    """
    Updates multiple items that match the given condition in a collection in a JSON database file.
    Update all the items if condition is empty = {}
    :param database_name:
    :param collection_name:
    :param condition:
    :param new_data:
    :return: True if the update was successful, False otherwise.
    """

    try:
        dict_condition = json.loads(condition)
        dict_new_data = json.loads(new_data)
    except json.decoder.JSONDecodeError:
        print('Update failed. Invalid JSON.')
        return False

    existing_file_number = get_last_file_number(database_name, collection_name)

    updated = False

    for i in range(1, existing_file_number+1):
        file_name = f'data/{database_name}_{collection_name}_{i}.json'
        try:
            with open(file_name, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f'File {file_name} not found.')
            return False

        if not dict_condition:
            for item in data:
                for key, value in dict_new_data.items():
                    item[key] = value
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            updated = True

        else:
            for item in data:
                if match_nested_condition(item, dict_condition):
                    update_nested_item(item, dict_condition, dict_new_data)
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=2)
            updated = True

    if updated:
        print('Update successful.')
        return True


if __name__ == '__main__':
    database = 'basketball'
    collection = 'test'
    # new_data = '{"PLAYER_NAME": "John Smith", "TEAM_ID": "0000", "PLAYER_ID": "0000", "SEASON": "2023"}'
    # insert_one(database, collection, new_data)
    # l_data = '[{"PLAYER_NAME": "John Smith", "TEAM_ID": "0000", "PLAYER_ID": "0000", "SEASON": "2023"}, {"PLAYER_NAME": "John Smith", "TEAM_ID": "0000", "PLAYER_ID": "0000", "SEASON": "2023"}]'
    # insert_many(database, collection, l_data)
    # condition = '{"PLAYER_NAME": "John Smith"}'
    # delete_one(database, collection, '{}')
    # delete_many(database, collection, '{}')
    # delete_many(database, collection, condition)
    update_data = '{"foo": "bar"}'
    # update_one(database, collection, '{"PLAYER_NAME": "John Smith"}', '{"REGION": "USA"}')
    update_many(database, collection, '{}', update_data)
    # update_many(database, collection, condition, update_data)