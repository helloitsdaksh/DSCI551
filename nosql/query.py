"""
Implement Read Operations
"""

import json
import os
import tempfile
import heapq
from collections import defaultdict


# -------------------------------------------
# Database and Collection Operations
# -------------------------------------------
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


# -------------------------------------------
# Data Filtering and Selection
# -------------------------------------------

def select_fields(temp_file, fields, last_operation: bool = True):
    """
    Selects specified fields from a single JSON record.
    :param temp_file: Path to temporary file.
    :param fields: List of fields to select from the record.
    :param last_operation: Whether the operation is the last operation in the pipeline.
    :return: A dictionary with only the selected fields.
    """
    new_temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')

    with open(temp_file, 'r') as file:
        for line in file:
            data = json.loads(line)
            selected_data = data if fields == ['*'] else {field: data[field] for field in fields}
            if last_operation:
                yield selected_data
            else:
                new_temp_file.write(json.dumps(selected_data) + '\n')

    os.remove(temp_file)
    new_temp_file.close()
    if last_operation:
        return
    return new_temp_file.name


def select_record_fields(record, fields):
    """
    Selects specified fields from a record.
    :param record: The record from which fields are to be selected.
    :param fields: List of fields to select from the record or '*' for all fields.
    :return: A dictionary with only the selected fields or all fields if '*' is specified.
    """
    # Select all fields if fields is '*'
    selected_record = record if fields == ['*'] else {field: record[field] for field in fields}
    return selected_record


def filter_by_values(temp_file, conditions) -> str:
    """
    Get all items in a collection that satisfy multiple given conditions.

    :param temp_file: Path to temporary file.
    :param conditions: Nested list of conditions or groups of conditions. Example: conditions =  {
        'operator': 'AND', 'conditions': [
            {'target': 'columnA', 'condition': 'gt', 'value': 1},
            {'operator': 'OR', 'conditions': [
                {'target': 'columnB', 'condition': 'eq', 'value': 'Foo'},
                {'target': 'columnC', 'condition': 'lt', 'value': 10}
            ]}
        ]}
    :return: Generator yielding items that satisfy all the conditions.
    """

    condition_functions = {
        'gt': lambda x, v: x > v,
        'lt': lambda x, v: x < v,
        'gte': lambda x, v: x >= v,
        'lte': lambda x, v: x <= v,
        'eq': lambda x, v: x == v,
        'ne': lambda x, v: x != v,
        'in': lambda x, v: x in v,
    }

    def evaluate_condition(item, condition):
        target, operator, value = condition['target'], condition['condition'], condition['value']
        condition_function = condition_functions.get(operator)
        if condition_function is None:
            raise ValueError(f'Invalid condition: {operator}')

        target_value = item.get(target)
        try:
            return target_value is not None and condition_function(target_value, value)
        except TypeError:
            return False

    def item_satisfies_conditions(item, conditions):
        if 'operator' in conditions and 'conditions' in conditions:
            operator = conditions['operator'].upper()
            sub_conditions = conditions['conditions']

            if operator == 'AND':
                return all(item_satisfies_conditions(item, cond) for cond in sub_conditions)
            elif operator == 'OR':
                return any(item_satisfies_conditions(item, cond) for cond in sub_conditions)
            else:
                raise ValueError(f'Invalid logical operator: {operator}')
        else:
            return evaluate_condition(item, conditions)

    with open(temp_file, 'r') as file:
        new_temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        for line in file:
            data = json.loads(line)
            if item_satisfies_conditions(data, conditions):
                json.dump(data, new_temp_file)
                new_temp_file.write('\n')  # Write each JSON object on a new line
        new_temp_file.close()

    os.remove(temp_file)
    return new_temp_file.name


# -------------------------------------------
# Sorting Operations
# -------------------------------------------
def _sort_and_write_chunk(file_name: str, sort_key: str | list[str], reverse=False):
    """
    Sorts and writes a chunk of data to a temporary file.
    :param file_name:
    :param sort_key:
    :param reverse:
    :return:
    """
    data = []

    # Read data from JSON lines file
    with open(file_name, 'r') as file:
        for line in file:
            data.append(json.loads(line))

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

    # Creating a temporary file to write the merged results
    merged_file = tempfile.NamedTemporaryFile(delete=False, mode='w')

    # Merge process
    while heap:
        _, file_index, smallest = heapq.heappop(heap)
        json.dump(smallest, merged_file)
        merged_file.write('\n')

        # Read next element from the same file
        line = files[file_index].readline().strip()
        if line:
            data = json.loads(line)
            _push_to_heap(heap, file_index, data, sort_key, reverse)

    # Close file objects
    for file in files:
        file.close()

    merged_file.close()
    return merged_file.name


def execute_external_sort(input_files: list[str], sort_key: str, reverse=False):
    """
    External sort operation on a collection.
    :param input_files:
    :param sort_key:
    :return:
    """
    sorted_files = [_sort_and_write_chunk(file_name, sort_key, reverse=reverse) for file_name in input_files]
    merged_file = _merge_sorted_files(sorted_files, sort_key, reverse=reverse)
    for temp_file in sorted_files:
        os.remove(temp_file)

    return merged_file


# -------------------------------------------
# Aggregation Functions
# -------------------------------------------

def partial_aggregate(temp_file_name: str, group_keys: str | list[str], targets: dict | list[dict]) -> dict:
    """
    Performs partial aggregation on a JSON file. It groups the data based on the specified keys and performs
    :param temp_file_name:
    :param group_keys:
    :param targets:
    :return:
    """
    grouped_data = defaultdict(lambda: {target: [] for target in targets})

    # if isinstance(targets, list):
    #     # Assuming each element in the list is a dictionary with one key-value pair
    #     targets = {list(target.keys())[0]: list(target.values())[0] for target in targets}

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
            # filter out non-numeric values for numeric aggregations
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            if aggregation == 'sum':
                partial_result[group_key][target] = sum(numeric_values)
            elif aggregation == 'avg':
                partial_result[group_key][target] = (
                sum(numeric_values), len(numeric_values))  # Store sum and count for average calculation
            elif aggregation == 'count':
                partial_result[group_key][target] = len(values)
            elif aggregation == 'max':
                partial_result[group_key][target] = max(numeric_values)
            elif aggregation == 'min':
                partial_result[group_key][target] = min(numeric_values)
            else:
                raise ValueError("Unsupported aggregation function")

    return partial_result


def final_aggregate(partial_results: list[dict], targets: dict | list[dict]) -> dict:
    """
    Performs final aggregation on partial results.
    :param partial_results:
    :param targets:
    :return:
    """
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
    formatted_data = []
    for group_keys, group_values in final_result.items():
        group_doc = {"_key": list(group_keys) if len(group_keys) > 1 else group_keys[0]}
        for target, data in group_values.items():
            aggregation = targets[target]
            agg_key = f'{target}_{aggregation}'
            if aggregation == 'avg':
                group_doc[agg_key] = round(data['sum'] / data['count'], 4) if data['count'] > 0 else 0
            elif aggregation in ['max', 'min']:
                group_doc[agg_key] = max(data['values']) if aggregation == 'max' else min(data['values'])
            else:
                group_doc[agg_key] = data['sum']
        formatted_data.append(group_doc)

    return formatted_data


# -------------------------------------------
# Utility Functions
# -------------------------------------------
def save_json_items_to_tempfile(input_file_path: str) -> str:
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


if __name__ == '__main__':
    metadata_file = 'metadata.json'
    database = 'nba'
    # test filtering
    # target = 'artist(s)_name'
    # condition = 'eq'
    # value = 'Ariana Grande'
    # result = query_manager.filter_by_value(collection, target, condition, value)
    # for item in result:
    #     print(item)

    # test group_by
    collection = 'players'
    group_key = ['season', 'tm']
    targets = {'player': 'count', 'age': 'avg'}
    file_number = get_last_file_number(metadata_file, database, collection)
    input_files = [f'data/{database}_{collection}_{i}.json' for i in range(1, file_number+1)]
    temp_files = [save_json_items_to_tempfile(input_file) for input_file in input_files]
    partial_results = [partial_aggregate(temp_file, group_key, targets) for temp_file in temp_files]
    result = final_aggregate(partial_results, targets)
    print(result)

    # test sort_by
    # input_files = ['../data/sample_test.json', '../data/sample_test_2.json']
    # output_file = '../data/sample_test_sorted.json'
    # sort_key = ['key1', 'key2']
    # query_manager.execute_external_sort(input_files, sort_key)

    # SEARCH player_name IN players FILTER season = '2018' LIMIT 10
    # collection = 'players'
    # file_number = get_last_file_number(metadata_file, database, collection)
    # input_files = [f'../data/{database}_{collection}_{i}.json' for i in range(1, file_number + 1)]
    # temp_files = [save_json_items_to_tempfile(input_file) for input_file in input_files]
    # filter_results = [filter_by_values(temp_file, [{'target': 'season', 'condition': 'eq', 'value': '2018'}]) for temp_file in temp_files]
    # results = [select_fields(filter_result, ['player_name', 'season'], last_operation=True) for filter_result in filter_results]
    # limit = 10
    # count = 0
    # for result in results:
    #     for item in result:
    #         count += 1
    #         print(item)
    #         if count >= limit:
    #             break

    # SEARCH game_id, points IN games FILTER pts_home > 130 LIMIT 10
    # collection = 'games'
    # file_number = get_last_file_number(metadata_file, database, collection)
    # input_files = [f'../data/{database}_{collection}_{i}.json' for i in range(1, file_number + 1)]
    # temp_files = [save_json_items_to_tempfile(input_file) for input_file in input_files]
    # filter_results = [filter_by_values(temp_file, [{'target': 'pts_home', 'condition': 'gt', 'value': 130}]) for temp_file in temp_files]
    # results = [select_fields(filter_result, ['game_id', 'pts_home'], last_operation=True) for filter_result in filter_results]
    # limit = 10
    # count = 0
    # for result in results:
    #     for item in result:
    #         count += 1
    #         print(item)
    #         if count >= limit:
    #             break

