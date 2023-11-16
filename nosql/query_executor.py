"""
Modify group by function, so it returns a dictionary format and can be passed onto another function
"""


from query import *


METADATA_FILE = 'metadata.json'


def parse_query(query):
    parts = query.split()
    parts_lower = query.lower().split()
    try:
        get_index = parts_lower.index("get")
        from_index = parts_lower.index("from")
    except ValueError:
        return None, None, None, None, None, None

    filter_index = parts_lower.index("filter") if "filter" in parts_lower else None
    group_index = parts_lower.index("group") if "group" in parts_lower else None
    sort_index = parts_lower.index("sort") if "sort" in parts_lower else None
    limit_index = parts_lower.index("limit") if "limit" in parts_lower else None

    conditions = []
    group_by = []
    sort_by = []
    limit = None

    # Extracting GET clause
    columns = ' '.join(parts[get_index + 1:from_index]).replace(',', '').split()

    # Extracting FROM clause
    table = parts[from_index + 1]

    # Function to process filter values
    def process_filter_value(value):
        if value.startswith('"') and value.endswith('"'):
            return value.strip('"')  # Remove quotes for strings
        try:
            return int(value) if '.' not in value else float(value)  # Convert to int or float
        except ValueError:
            return value  # Return as is if conversion fails

    # Extracting FILTER clause
    if filter_index:
        filter_end = group_index or sort_index or limit_index or len(parts)
        filter_parts = parts[filter_index + 1:filter_end]
        conditions = [process_filter_value(val) for val in filter_parts]

    # Extracting GROUP clause
    if group_index:
        group_end = sort_index or limit_index or len(parts)
        group_by = ' '.join(parts[group_index + 1:group_end]).replace(',', '').split()

    # Extracting SORT clause
    if sort_index:
        sort_end = limit_index or len(parts)
        sort_by = ' '.join(parts[sort_index + 1:sort_end]).replace(',', '').split()
    print()

    # Extracting LIMIT clause
    if limit_index:
        limit = int(parts[limit_index + 1])

    return columns, table, conditions, group_by, sort_by, limit


def convert_to_nested_format(condition_list):
    operator_mapping = {
        '>': 'gt',
        '<': 'lt',
        '>=': 'gte',
        '<=': 'lte',
        '=': 'eq',
        '!=': 'ne',
        'in': 'in'
    }
    operator_flip = {
        '>': '<',
        '<': '>',
        '>=': '<=',
        '<=': '>=',
    }

    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def handle_condition(sublist):
        first, operator_symbol, second = sublist

        if is_number(first):
            # Flip the operator if number/string precedes the column name
            operator_symbol = operator_flip.get(operator_symbol, operator_symbol)
            target, value = second, first
        else:
            target, value = first, second

        if isinstance(value, str):
            value = value.strip('\'"')

        operator = operator_mapping[operator_symbol]
        return {'target': target, 'condition': operator, 'value': value}

    conditions = []
    current_operator = None
    i = 0

    while i < len(condition_list):
        if condition_list[i].upper() in ['AND', 'OR']:
            if current_operator and conditions:
                conditions = [{'operator': current_operator, 'conditions': conditions}]
            current_operator = condition_list[i].upper()
            i += 1
            continue

        condition = handle_condition(condition_list[i:i + 3])
        conditions.append(condition)
        i += 3

    if current_operator:
        return {'operator': current_operator, 'conditions': conditions}
    else:
        return conditions[0]


def execute_query(database, query):
    try:
        columns, table, conditions, group_by, sort_by, limit = parse_query(query)
    except ValueError:
        print("Invalid query")
        return

    if not columns:
        print("Invalid query")
        return

    file_number = get_last_file_number(METADATA_FILE, database, table)
    input_files = [f'data/{database}_{table}_{i}.json' for i in range(1, file_number + 1)]
    intermediate_results = [save_json_items_to_tempfile(input_file) for input_file in input_files]

    try:
        if conditions:
            converted_conditions = convert_to_nested_format(conditions)
            intermediate_results = [filter_by_values(temp_file, converted_conditions) for temp_file in intermediate_results]

        if sort_by:
            sorted_file = execute_external_sort(intermediate_results, sort_by)
            with open(sorted_file, 'r') as file:
                for line in file:
                    print(select_record_fields(json.loads(line), columns))
            os.remove(sorted_file)
        else:
            results = [select_fields(temp_file, columns) for temp_file in intermediate_results]
            for result in results:
                for item in result:
                    print(item)
    finally:
        # Clean up all intermediate files
        for temp_file in intermediate_results:
            if os.path.exists(temp_file):
                os.remove(temp_file)


if __name__ == '__main__':
    query = "GET season, player, age FROM players FILTER season = '2024' AND age >= 32 SORT age"
    execute_query('nba', query)