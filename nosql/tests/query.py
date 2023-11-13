"""
Implement Read Operations
"""

import json
import os
import tempfile
import heapq
import sys
from collections import defaultdict


def _load_metadata(metadata_file):
    """
    Load metadata from a JSON file.
    :return: database metadata
    """
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as file:
            return json.load(file)
    else:
        return {}


def get_last_file_number(metadata_file, database_name: str, collection_name: str) -> list or None:
    """
    Returns the last file number of a collection in a JSON database file.
    :param database_name: The name of the database.
    :param collection_name: The name of the collection within the database.
    :return: The last file number or None if the collection does not exist.
    """
    metadata = _load_metadata(metadata_file)
    try:  # should check if the collection exists first
        for database in metadata['databases']:
            if database['name'] == database_name:
                for collection in database['collections']:
                    if collection['name'] == collection_name:
                        file_number = collection['partition_count']
                        return file_number
    except KeyError:
        return None

# def _load_data(self, collection_name):
#     """
#     Load data from a specified collection in the database.
#
#     :param collection_name: Name of the collection.
#     :return: Loaded data as a list or None if the file is not found.
#     """
#     file_name = f'../data/{self.database_name}_{collection_name}.json'
#     try:
#         with open(file_name, 'r') as file:
#             data = json.load(file)
#         return data
#     except FileNotFoundError:
#         return None


def filter_by_value(data, target, condition, value):
    """
    Get all items in a collection that have a value satisfying the given condition.

    :param data: dictionary or list of items.
    :param target: Name of the target field.
    :param condition: Comparison operator (e.g., gt, lt, gte, lte, eq, ne, in).
    :param value: Value to compare.
    :return: Generator yielding items that satisfy the condition.
    """

    conditions = {
        'gt': lambda x: x > value,
        'lt': lambda x: x < value,
        'gte': lambda x: x >= value,
        'lte': lambda x: x <= value,
        'eq': lambda x: x == value,
        'ne': lambda x: x != value,
        'in': lambda x: value in x
    }

    condition_function = conditions.get(condition)
    if condition_function is None:
        print('Invalid condition.')
        return False

    for item in data:
        target_value = item.get(target)
        if target_value is not None and condition_function(target_value):
            yield item


def _sort_and_write_chunk(file_name: str, sort_key: str | list[str], reverse=False):
    """
    Sorts and writes a chunk of data to a temporary file.
    :param file_name:
    :param sort_key:
    :param reverse:
    :return:
    """
    with open(file_name, 'r') as file:
        data = json.load(file)

    if isinstance(sort_key, list):
        data.sort(key=lambda x: tuple(x[k] for k in sort_key), reverse=reverse)
    else:
        data.sort(key=lambda x: x[sort_key], reverse=reverse)  # Sorting based on the specified key

    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
    for item in data:
        json.dump(item, temp_file)
        temp_file.write('\n')  # Write each JSON object on a new line
    temp_file.close()
    return temp_file.name


def _push_to_heap(heap, file_index, data, sort_key, reverse):
    """
    Pushes data to the heap
    Used in _merge_sorted_files
    :param heap:
    :param file_index:
    :param data:
    :param sort_key:
    :param reverse:
    :return:
    """
    if isinstance(sort_key, list):
        key = tuple(-data[k] if reverse else data[k] for k in sort_key)
    else:
        key = -data[sort_key] if reverse else data[sort_key]
    heapq.heappush(heap, (key, file_index, data))


def _merge_sorted_files(sorted_files: list, sort_key: str | list[str], reverse=False):
    """
    Merges sorted JSON files into a single sorted JSON file
    1. The first item from each sorted file will be pushed to the heap
    2. The smallest item from the heap will be popped and printed to the console
    3. The next item from the same file will be pushed to the heap
    4. The process continues until all items have been processed
    :param sorted_files:
    :param sort_key: string or list of strings representing the keys to sort on
    :param reverse: False for ascending order, True for descending order (default: False)
    :return:
    """
    files = [open(file, 'r') for file in sorted_files]
    heap = []

    # Initial population of the heap
    for file_index, file in enumerate(files):
        line = file.readline().strip()
        if line:
            data = json.loads(line)
            _push_to_heap(heap, file_index, data, sort_key, reverse)

    # Merge process
    while heap:
        _, file_index, smallest = heapq.heappop(heap)
        print(json.dumps(smallest))  # Print each merged item to the console

        # Read next element from the same file
        line = files[file_index].readline().strip()
        if line:
            data = json.loads(line)
            _push_to_heap(heap, file_index, data, sort_key, reverse)

    # Close file objects
    for file in files:
        file.close()


def execute_external_sort(input_files: list[str], sort_key: str, reverse=False):
    """
    External sort operation on a collection.
    :param input_files:
    :param sort_key:
    :return:
    """
    sorted_files = [_sort_and_write_chunk(file_name, sort_key, reverse=reverse) for file_name in input_files]
    _merge_sorted_files(sorted_files, sort_key, reverse=reverse)
    for temp_file in sorted_files:
        os.remove(temp_file)


def save_json_items_to_tempfile(input_file_path):
    """
    Reads a JSON file and writes its items to a temporary file, one item per line.
    This is for testing purposes.
    It can be also used to pass jsonlines as input to operations.
    :param input_file_path: Path to the input JSON file.
    :return: Path to the created temporary file.
    """
    # Read the input file
    with open(input_file_path, 'r') as file:
        data = json.load(file)

    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')

    # Write each JSON item on a separate line
    for item in data:
        json.dump(item, temp_file)
        temp_file.write('\n')

    temp_file.close()
    return temp_file.name


def partial_aggregate(temp_file_name, group_keys, targets):
    grouped_data = defaultdict(lambda: {target: [] for target in targets})

    with open(temp_file_name, 'r') as file:
        for line in file:
            record = json.loads(line.strip())
            group_key_values = tuple(record[key] for key in group_keys) if isinstance(group_keys, list) else (
            record[group_keys],)
            for target, aggregation in targets.items():
                if target in record:
                    grouped_data[group_key_values][target].append(record[target])

    # Perform partial aggregation
    partial_result = {}
    for group_key, group in grouped_data.items():
        partial_result[group_key] = {}
        for target, values in group.items():
            aggregation = targets[target]
            if aggregation == 'sum':
                partial_result[group_key][target] = sum(values)
            elif aggregation == 'avg':
                partial_result[group_key][target] = (
                sum(values), len(values))  # Store sum and count for average calculation
            elif aggregation == 'count':
                partial_result[group_key][target] = len(values)
            elif aggregation == 'max':
                partial_result[group_key][target] = max(values)
            elif aggregation == 'min':
                partial_result[group_key][target] = min(values)
            else:
                raise ValueError("Unsupported aggregation function")

    return partial_result


def final_aggregate(partial_results, targets):
    final_result = defaultdict(lambda: {target: {'sum': 0, 'count': 0, 'values': []} for target in targets})

    for partial in partial_results:
        for group_key, group_values in partial.items():
            for target, value in group_values.items():
                aggregation = targets[target]
                if aggregation in ['sum', 'count']:
                    final_result[group_key][target]['sum'] += value
                elif aggregation == 'avg':
                    final_result[group_key][target]['sum'] += value[0]
                    final_result[group_key][target]['count'] += value[1]
                elif aggregation in ['max', 'min']:
                    final_result[group_key][target]['values'].append(value)

    # Calculate final aggregated values
    for group_key, group_values in final_result.items():
        for target, data in group_values.items():
            aggregation = targets[target]
            if aggregation == 'avg':
                final_result[group_key][target] = data['sum'] / data['count'] if data['count'] > 0 else 0
            elif aggregation in ['max', 'min']:
                final_result[group_key][target] = max(data['values']) if aggregation == 'max' else min(
                    data['values'])
            else:
                final_result[group_key][target] = data['sum']

    formatted_data = {key[0] if len(key) == 1 else key: value for key, value in final_result.items()}

    return formatted_data


def select_fields(record, fields):
    """
    Selects specified fields from a single JSON record.
    :param record: A single JSON record (a dictionary).
    :param fields: List of fields to select from the record.
    :return: A dictionary with only the selected fields.
    """
    # Select only the specified fields
    return {field: record[field] for field in fields if field in record}


if __name__ == '__main__':
    metadata_file = 'metadata.json'
    database = 'basketball'
    collection = 'games'
    # test filtering
    # target = 'artist(s)_name'
    # condition = 'eq'
    # value = 'Ariana Grande'
    # result = query_manager.filter_by_value(collection, target, condition, value)
    # for item in result:
    #     print(item)

    # test group_by
    group_key = ['SEASON', 'HOME_TEAM_ID']
    targets = {'GAME_ID': 'count'}
    file_number = get_last_file_number(metadata_file, database, collection)
    input_files = [f'../data/{database}_{collection}_{i}.json' for i in range(1, file_number+1)]
    temp_files = [save_json_items_to_tempfile(input_file) for input_file in input_files]
    partial_results = [partial_aggregate(temp_file, group_key, targets) for temp_file in temp_files]
    result = final_aggregate(partial_results, targets)
    print(result)

    # test sort_by
    # input_files = ['../data/sample_test.json', '../data/sample_test_2.json']
    # output_file = '../data/sample_test_sorted.json'
    # sort_key = ['key1', 'key2']
    # query_manager.execute_external_sort(input_files, sort_key)