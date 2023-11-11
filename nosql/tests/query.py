"""
Implement Read Operations
"""

import json
from collections import defaultdict


class DataQueryManager:
    def __init__(self, database_name):
        """
        Initialize the DataQueryManager with the given database name.

        :param database_name: Name of the database.
        """
        self.database_name = database_name

    def _load_data(self, collection_name):
        """
        Load data from a specified collection in the database.

        :param collection_name: Name of the collection.
        :return: Loaded data as a list or None if the file is not found.
        """
        file_name = f'../data/{self.database_name}_{collection_name}.json'
        try:
            with open(file_name, 'r') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return None

    def filter_by_value(self, collection_name, target, condition, value):
        """
        Get all items in a collection that have a value satisfying the given condition.

        :param collection_name: Name of the collection.
        :param target: Name of the target field.
        :param condition: Comparison operator (e.g., gt, lt, gte, lte, eq, ne, in).
        :param value: Value to compare.
        :return: Generator yielding items that satisfy the condition.
        """
        data = self._load_data(collection_name)
        if data is None:
            return False

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

    def sort_by(self, collection_name, target, ascending=True):
        """
        Sort operation on a collection.

        :param collection_name: Name of the collection.
        :param target: Name of the target field.
        :param ascending: Boolean indicating whether to sort in ascending order.
        :return: List of sorted records.
        """
        data = self._load_data(collection_name)
        if data is None:
            return False

        sorted_data = sorted(data, key=lambda x: x[target], reverse=not ascending)
        return sorted_data

    def group_by(self, collection_name, group_keys, target, aggregation):
        """
        Group by operation on a collection.
        THIS FUNCTION ALSO AGGREGATES FOR MISSING OR NULL VALUES.
        :param collection_name: Name of the collection.
        :param group_keys: Fields to group by.
        :param target: Field to aggregate.
        :param aggregation: Aggregation function (e.g., 'sum', 'avg', 'count', 'max', 'min').
        :return: Dictionary containing the grouped and aggregated results.
        """
        data = self._load_data(collection_name)
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
    database = 'spotify'
    collection = 'sample'
    query_manager = DataQueryManager(database)
    # test filtering
    # target = 'artist(s)_name'
    # condition = 'eq'
    # value = 'Ariana Grande'
    # result = query_manager.filter_by_value(collection, target, condition, value)
    # for item in result:
    #     print(item)

    # test group_by
    group_key = ['key', 'mode']
    target = 'bpm'
    aggregation = 'count'
    result = query_manager.group_by(collection, group_key, target, aggregation)
    print(result)

    # test sort_by
    # target = 'bpm'
    # result = sort_by(database, collection, target, ascending=True)
    # print(result)
