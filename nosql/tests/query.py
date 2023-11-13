"""
Implement Read Operations
"""

import json
import os
import tempfile
import heapq
import sys
from collections import defaultdict


class DataQueryManager:
    def __init__(self, database_name):
        """
        Initialize the DataQueryManager with the given database name.

        :param database_name: Name of the database.
        """
        self.database_name = database_name
        self.metadata_file = 'metadata.json'

    def _load_metadata(self):
        """
        Load metadata from a JSON file.
        :return: database metadata
        """
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as file:
                return json.load(file)
        else:
            return {}

    def _get_last_file_number(self, database_name: str, collection_name: str) -> list or None:
        """
        Returns the last file number of a collection in a JSON database file.
        :param database_name: The name of the database.
        :param collection_name: The name of the collection within the database.
        :return: The last file number or None if the collection does not exist.
        """
        metadata = self._load_metadata()
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

    def filter_by_value(self, data, target, condition, value):
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

    def _sort_and_write_chunk(self, file_name: str, sort_key: str | list[str], reverse=False):
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

    def _push_to_heap(self, heap, file_index, data, sort_key, reverse):
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

    def _merge_sorted_files(self, sorted_files: list, sort_key: str | list[str], reverse=False):
        """
        Merges sorted JSON files into a single sorted JSON file
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
                self._push_to_heap(heap, file_index, data, sort_key, reverse)

        # Merge process
        while heap:
            _, file_index, smallest = heapq.heappop(heap)
            print(json.dumps(smallest))  # Print each merged item to the console

            # Read next element from the same file
            line = files[file_index].readline().strip()
            if line:
                data = json.loads(line)
                self._push_to_heap(heap, file_index, data, sort_key, reverse)

        # Close file objects
        for file in files:
            file.close()

    def execute_external_sort(self, input_files: list[str], sort_key: str, reverse=False):
        """
        External sort operation on a collection.
        :param input_files:
        :param sort_key:
        :return:
        """
        sorted_files = [self._sort_and_write_chunk(file_name, sort_key, reverse=reverse) for file_name in input_files]
        self._merge_sorted_files(sorted_files, sort_key, reverse=reverse)
        for temp_file in sorted_files:
            os.remove(temp_file)

    def group_by(self, data, group_keys, target, aggregation):
        """
        Group by operation on a collection.
        THIS FUNCTION ALSO AGGREGATES FOR MISSING OR NULL VALUES.
        :param data: dictionary or list of items.
        :param group_keys: Fields to group by.
        :param target: Field to aggregate.
        :param aggregation: Aggregation function (e.g., 'sum', 'avg', 'count', 'max', 'min').
        :return: Dictionary containing the grouped and aggregated results.
        """

        if data is None:
            return False

        grouped_data = defaultdict(lambda: {'values': []})

        for record in data:
            group_key_values = tuple(record[key] for key in group_keys) if isinstance(group_keys, list) else (
            record[group_keys],)
            grouped_data[group_key_values]['values'].append(record[target])

        result = {}
        for group_key, group in grouped_data.items():
            values = group['values']
            if aggregation == 'sum':
                result[group_key] = sum(values)
            elif aggregation == 'avg':
                result[group_key] = sum(values) / len(values) if len(values) > 0 else 0
            elif aggregation == 'count':
                result[group_key] = len(values)
            elif aggregation == 'max':
                result[group_key] = max(values)
            elif aggregation == 'min':
                result[group_key] = min(values)
            else:
                return False  # Unsupported aggregation function

        return result


if __name__ == '__main__':
    database = 'sample'
    # collection = 'sample'
    query_manager = DataQueryManager(database)
    # test filtering
    # target = 'artist(s)_name'
    # condition = 'eq'
    # value = 'Ariana Grande'
    # result = query_manager.filter_by_value(collection, target, condition, value)
    # for item in result:
    #     print(item)

    # test group_by
    # group_key = ['key', 'mode']
    # target = 'bpm'
    # aggregation = 'count'
    # result = query_manager.group_by(collection, group_key, target, aggregation)
    # print(result)

    # test sort_by
    # target = 'bpm'
    # result = sort_by(database, collection, target, ascending=True)
    # print(result)

    input_files = ['../data/sample_test.json', '../data/sample_test_2.json']
    output_file = '../data/sample_test_sorted.json'
    sort_key = ['key1', 'key2']
    query_manager.execute_external_sort(input_files, sort_key)