import ast
import os
import csv
import json
import tempfile
import re

class Database:
    def __init__(self, table_folder='tables', metadata_file='metadata.json'):
        self.table_folder = table_folder
        self.metadata_file = metadata_file
        self.tables = {}  # A dictionary to store table metadata

        if not os.path.exists(self.table_folder):
            os.makedirs(self.table_folder)

        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as json_file:
                self.tables = json.load(json_file)

    def update_references(self):
        references = {}  # Initialize the references dictionary

        # Iterate through the table metadata
        for table_name, table_metadata in self.tables.items():
            foreign_keys = table_metadata.get('foreign_keys')
            if foreign_keys:
                for foreign_key in foreign_keys:
                    for column, referenced_table in foreign_key.items():
                        # Remove the extension from the table names
                        referenced_table = self.remove_extension(referenced_table)
                        table_name_without_extension = self.remove_extension(table_name)
                        if referenced_table not in references:
                            references[referenced_table] = []  # Add the referenced table without extension
                        references[referenced_table].append(table_name_without_extension)

        # Load the existing metadata from the metadata file
        with open(self.metadata_file, 'r') as metadata_json_file:
            metadata = json.load(metadata_json_file)

        # Update the "references" section in the metadata
        metadata['references'] = references

        # Save the updated metadata with references to the metadata file
        with open(self.metadata_file, 'w') as metadata_json_file:
            json.dump(metadata, metadata_json_file, indent=4)

    def create_table(self, table_name, columns, primary_key=None, unique_constraints=None, foreign_keys=None):
        table_name = self.add_extension(table_name)

        if table_name in self.tables:
            print(f"Table '{table_name}' already exists.")
            return False

        table_metadata = {
            'table_name': table_name,
            'columns': columns,
            'primary_key': primary_key,
            'unique_constraints': unique_constraints,
            'foreign_keys': foreign_keys,  # Include foreign_keys in the table's metadata
        }

        # Create a CSV file for the new table
        table_file_path = os.path.join(self.table_folder, table_name)
        with open(table_file_path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=columns.keys())
            writer.writeheader()

        self.tables[table_name] = table_metadata

        # Save the updated metadata to the metadata file
        with open(self.metadata_file, 'w') as json_file:
            json.dump(self.tables, json_file, indent=4)

        self.update_references()

        print(f"Table '{table_name}' created successfully.")
        return True

    def insert(self, table_name, data):
        table_name = self.add_extension(table_name)

        if table_name not in self.tables:
            print(f"Error: Table '{table_name}' does not exist.")
            return False

        table_metadata = self.tables[table_name]
        headers = list(table_metadata["columns"].keys())
        primary_key = table_metadata.get("primary_key")
        unique_constraints = table_metadata.get("unique_constraints")
        foreign_keys = table_metadata.get("foreign_keys")

        for header, value in data.items():
            expected_data_type = table_metadata["columns"].get(header)
            if not self.validate_data_type(value, expected_data_type):
                print(f"Error: Data type violation for column '{header}'. Expected {expected_data_type}.")
                return False

        temp_table_file_path = os.path.join(self.table_folder, 'temp_insert_file.csv')
        table_file_path = os.path.join(self.table_folder, table_name)

        with open(table_file_path, mode='r', newline='') as csv_file, open(temp_table_file_path, mode='w', newline=''
                                                                           ) as temp_file:
            reader = csv.DictReader(csv_file)
            writer = csv.DictWriter(temp_file, fieldnames=headers)
            writer.writeheader()

            existing_data = list(reader)
            foreign_key_check = True

            if primary_key and any(row[primary_key] == data.get(primary_key) for row in existing_data):
                print(f"Error: Primary key constraint violated for column '{primary_key}'.")
                return False

            if unique_constraints:
                for constraint in unique_constraints:
                    if any(row.get(constraint) == data.get(constraint) for row in existing_data):
                        print(f"Error: Unique constraint violated for column '{constraint}'.")
                        return False

            if foreign_keys:
                for foreign_key in foreign_keys:
                    for foreign_key_column, referenced_table in foreign_key.items():
                        referenced_table = referenced_table.replace(".csv","")
                        referenced_metadata = self.tables.get(referenced_table)
                        if referenced_metadata:
                            referenced_column = referenced_metadata.get("primary_key")
                            if not referenced_column:
                                print(f"Error: Referenced table '{referenced_table}' has no primary key.")
                                return False

                            referenced_table_path = os.path.join(self.table_folder, referenced_table + '.csv')

                            with open(referenced_table_path, mode='r', newline='') as ref_csv_file:
                                ref_reader = csv.DictReader(ref_csv_file)
                                ref_data = list(ref_reader)

                            if not any(row[referenced_column] == data.get(foreign_key_column) for row in ref_data):
                                foreign_key_check = False
                                break

            if foreign_keys and not foreign_key_check:
                print(f"Error: Foreign key constraint violated.")
                return False

            writer.writerows(existing_data)
            writer.writerow(data)

        os.remove(table_file_path)
        os.rename(temp_table_file_path, table_file_path)

        return print(f"Data inserted into table '{table_name}' successfully.")

    def update(self, table_name, condition, new_data):
        table_name = self.add_extension(table_name)

        if table_name not in self.tables:
            print(f"Error: Table '{table_name}' does not exist.")
            return False

        table_metadata = self.tables[table_name]
        headers = list(table_metadata["columns"].keys())
        primary_key = table_metadata.get("primary_key")
        foreign_keys = table_metadata.get("foreign_keys")

        # Ensure that the condition and new_data are dictionaries with a single key-value pair
        if not isinstance(condition, dict) or len(condition) != 1 or not isinstance(new_data, dict) or len(new_data
                                                                                                           ) < 1:
            print("Error: Invalid condition or new_data format.")
            return False

        # Extract the column and value from the condition
        column, value = condition.popitem()

        # Validate the column name
        if column not in headers:
            print(f"Error: Column '{column}' does not exist in the table.")
            return False

        temp_table_file_path = os.path.join(self.table_folder, 'temp_update_file.csv')
        table_file_path = os.path.join(self.table_folder, table_name)

        data_updated = False
        foreign_key_check = True

        with open(table_file_path, mode='r', newline='') as csv_file, open(temp_table_file_path, mode='w',
                                                                           newline=''
                                                                           ) as temp_file:
            reader = csv.DictReader(csv_file)
            writer = csv.DictWriter(temp_file, fieldnames=headers)
            writer.writeheader()

            for row in reader:
                if row[column] == str(value) or row[column] == value:
                    # Check foreign key constraints for the updated foreign key column
                    if foreign_keys:
                        for foreign_key in foreign_keys:
                            for foreign_key_column, referenced_table in foreign_key.items():
                                referenced_column = self.tables[referenced_table+".csv"].get("primary_key")
                                if not referenced_column:
                                    print(f"Error: Referenced table '{referenced_table}' has no primary key.")
                                    return False

                                # If the foreign key column is being updated, check the constraint
                                if foreign_key_column in new_data:
                                    referenced_table_path = os.path.join(self.table_folder,
                                                                         referenced_table + '.csv'
                                                                         )

                                    with open(referenced_table_path, mode='r', newline='') as ref_csv_file:
                                        ref_reader = csv.DictReader(ref_csv_file)
                                        # Check if the combined data violates foreign key constraints
                                        if new_data[foreign_key_column] not in [row[referenced_column] for row in
                                                                                ref_reader]:
                                            foreign_key_check = False
                                            break

                    # If foreign key checks pass, update the row with the new data
                    if foreign_key_check:
                        row.update(new_data)
                        writer.writerow(row)
                        data_updated = True
                else:
                    writer.writerow(row)

        if not foreign_key_check:
            print("Error: Foreign key constraint violated. Update not performed.")
            return False
        if not data_updated:
            print("Error: No data matched the condition for update.")
            return False
        # Replace the original table file with the temporary file
        os.remove(table_file_path)
        os.rename(temp_table_file_path, table_file_path)

        return print(f"Data updated in table '{table_name}' successfully.")

    def delete(self, table_name, condition, force=False):
        table_name = self.add_extension(table_name)

        if table_name not in self.tables:
            print(f"Error: Table '{table_name}' does not exist.")
            return False

        table_metadata = self.tables[table_name]

        # Check if the table is referenced by other tables
        references = self.tables.get("references", {})

        table_name_without_extension = table_name.replace('.csv', '')  # Remove the ".csv" extension

        if table_name_without_extension in references:
            referenced_tables = references[table_name_without_extension]
            for ref_table in referenced_tables:
                referenced_table_metadata = self.tables.get(ref_table + '.csv')  # Add the ".csv" extension
                if referenced_table_metadata:
                    referenced_foreign_key = referenced_table_metadata.get("foreign_keys")
                    if referenced_foreign_key:
                        # Check if the referenced table's foreign key references this table's record
                        for foreign_key in referenced_foreign_key:
                            for foreign_key_column, referenced_table in foreign_key.items():
                                if referenced_table == table_name.replace(".csv", ""):
                                    if not force:
                                        print(
                                            f"Error: Records from '{table_name.replace('.csv', '')}' are referenced in '{ref_table}'. Deletion is not allowed."
                                            )
                                        return False  # Return False instead of breaking

        temp_table_file_path = os.path.join(self.table_folder, 'temp_delete_file.csv')
        table_file_path = os.path.join(self.table_folder, table_name)

        delete_successful = False

        with open(table_file_path, mode='r', newline='') as csv_file, open(temp_table_file_path, mode='w',
                                                                           newline=''
                                                                           ) as temp_file:
            reader = csv.DictReader(csv_file)
            writer = csv.DictWriter(temp_file, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                if not all(row.get(key) == str(value) for key, value in condition.items()):
                    writer.writerow(row)
                else:
                    delete_successful = True  # Moved inside the loop

        if delete_successful and force:
            # Perform the force delete
            os.remove(table_file_path)
            os.rename(temp_table_file_path, table_file_path)

            # Delete from the referenced table as well
            for ref_table in referenced_tables:
                ref_table_file_path = os.path.join(self.table_folder, ref_table + '.csv')
                temp_ref_table_file_path = os.path.join(self.table_folder, 'temp_delete_file_ref.csv')

                with open(ref_table_file_path, mode='r', newline='') as ref_csv_file, open(temp_ref_table_file_path,
                                                                                           mode='w', newline=''
                                                                                           ) as temp_ref_file:
                    ref_reader = csv.DictReader(ref_csv_file)
                    ref_writer = csv.DictWriter(temp_ref_file, fieldnames=ref_reader.fieldnames)
                    ref_writer.writeheader()

                    for row in ref_reader:
                        if not all(row.get(key) == value for key, value in condition.items()):
                            ref_writer.writerow(row)

                os.remove(ref_table_file_path)
                os.rename(temp_ref_table_file_path, ref_table_file_path)

        else:
            os.remove(temp_table_file_path)
        if delete_successful:
            print(f"Data deleted from table '{table_name}' successfully.")
        else:
            print(f"Data deletion from table '{table_name}' was not successful. since the condition was not met.")

        return

    def sort_file(self, input_filename, output_filename, sort_column):
        with open(os.path.join(self.table_folder, input_filename), 'r', newline='') as input_file:
            reader = csv.DictReader(input_file)
            sorted_rows = sorted(reader, key=lambda x: x[sort_column])

        with open(os.path.join(self.table_folder, output_filename), 'w', newline='', encoding='utf-8') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(sorted_rows)

    def external_sort_merge(self, table_name, sort_column):
        sorted_filename = f"{table_name}_sorted.csv"
        self.sort_file(table_name, sorted_filename, sort_column)
        return sorted_filename

    def apply_condition(self, row, condition):
        key, operator, value = condition

        if key not in row:
            return False

        if operator not in ["like", "startswith", "endswith"] and isinstance(row[key], str) and row[key].isdigit():
            value = int(value)
        # Convert the value to the appropriate type if it's a numeric column
        if isinstance(row[key], str):
            if operator == "like":
                if value not in row[key]:
                    return False
            elif operator == "startswith":
                if not row[key].startswith(value):
                    return False
            elif operator == "endswith":
                if not row[key].endswith(value):
                    return False
            elif operator == "ilike":
                if value not in row[key].lower():
                    return False
            elif operator == "icontains":
                if value not in row[key].lower():
                    return False
            elif operator == ">":
                if not int(row[key]) > value:
                    return False
            elif operator == "<":
                if not int(row[key]) < value:
                    return False
            elif operator == "==":
                if not int(row[key]) == value:
                    return False
            elif operator == ">=":
                if not int(row[key]) >= value:
                    return False
            elif operator == "<=":
                if not int(row[key]) <= value:
                    return False

        return True

    def join_with_condition(self, table_name1, table_name2, on_column, user_condition=None, selected_columns=None):
        table_name1 = self.add_extension(table_name1)
        table_name2 = self.add_extension(table_name2)
        temp_filename = "temp_result.csv"

        with open(os.path.join(self.table_folder, temp_filename), 'w', newline='', encoding='utf-8') as temp_file:
            # Get the fieldnames from the first row of reader1 and reader2
            with open(os.path.join(self.table_folder, table_name1), 'r', newline='') as csv_file1, \
                    open(os.path.join(self.table_folder, table_name2), 'r', newline='') as csv_file2:

                reader1 = csv.DictReader(csv_file1)
                reader2 = csv.DictReader(csv_file2)

                fieldnames = reader1.fieldnames + reader2.fieldnames

                # Determine the columns to write (selected_columns or all columns)
                write_columns = selected_columns if selected_columns else fieldnames

                temp_writer = csv.DictWriter(temp_file, fieldnames=write_columns)
                temp_writer.writeheader()

                for row1 in reader1:
                    csv_file2.seek(0)  # Reset the second CSV file to the beginning
                    for row2 in reader2:
                        if row1[on_column] == row2[on_column] and (
                                user_condition is None or self.apply_condition(row1, user_condition[0])):
                            joined_row = {**row1, **row2}
                            selected_row = {col: joined_row[col] for col in write_columns}
                            print(selected_row)
                            temp_writer.writerow(selected_row)

        self.external_sort_merge(temp_filename, on_column)

        return temp_filename

    def print_temp_file(self, file_path):
        with open(file_path, 'r', newline='') as temp_file:
            reader = csv.DictReader(temp_file)
            for row in reader:
                print(row)
    def search(self, table_name, conditions=None, columns=None, sort_by=None, ascending=True, group_by=None):
        table_name = self.add_extension(table_name)

        if table_name not in self.tables:
            print(f"Error: Table '{table_name}' does not exist.")
            return

        result_temp_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
        if columns == None:
            columns = self.tables[table_name]['columns'].keys()
        result_csv_writer = csv.DictWriter(result_temp_file, fieldnames=columns)
        result_csv_writer.writeheader()

        with open(os.path.join(self.table_folder, table_name), 'r', newline='') as csv_file:
            reader = csv.DictReader(csv_file)

            rows = []
            for idx, row in enumerate(reader):
                if conditions is None or all(self.check_condition(row, condition) for condition in conditions):
                    rows.append({key: row[key] for key in columns})

            if sort_by and sort_by in columns:
                rows.sort(key=lambda x: x[sort_by], reverse=not ascending)

                # Group by if a group_by column is specified
            if group_by and group_by in columns:
                grouped_data = {}
                for row in rows:
                    group_value = row[group_by]
                    if group_value not in grouped_data:
                        grouped_data[group_value] = []
                    grouped_data[group_value].append(row)

                # Write the grouped rows to the result file
                for group_value, group_rows in grouped_data.items():
                    result_csv_writer.writerow({group_by:group_value})
                    for group_row in group_rows:
                        result_csv_writer.writerow(group_row)

            else:
                # Write the sorted or unsorted rows to the result file
                for row in rows:
                    result_csv_writer.writerow(row)

        result_temp_file.close()

        self.print_temp_file(result_temp_file.name)  # Print the result

        # Read the grouped data from the file and continue processing
        with open(result_temp_file.name, 'r') as result_file:
            grouped_data_reader = csv.DictReader(result_file)
            for group_row in grouped_data_reader:
                # Continue processing grouped data as needed
                print(group_row)

        os.remove(result_temp_file.name)

    def check_condition(self, row, condition):
        key, operator, value = condition

        if key not in row:
            return False

        if operator not in["like", "startswith", "endswith"] and isinstance(row[key], str) and row[key].isdigit():
            value = int(value)
        # Convert the value to the appropriate type if it's a numeric column
        if isinstance(row[key], str):
            if operator == "like":
                if value not in row[key]:
                    return False
            elif operator == "startswith":
                if not row[key].startswith(value):
                    return False
            elif operator == "endswith":
                if not row[key].endswith(value):
                    return False
            elif operator == "ilike":
                if value not in row[key].lower():
                    return False
            elif operator == "icontains":
                if value not in row[key].lower():
                    return False
            elif operator == ">":
                if not int(row[key]) > value:
                    return False
            elif operator == "<":
                if not int(row[key]) < value:
                    return False
            elif operator == "==":
                if not int(row[key]) == value:
                    return False
            elif operator == ">=":
                if not int(row[key]) >= value:
                    return False
            elif operator == "<=":
                if not int(row[key]) <= value:
                    return False

        return True
    def add_extension(self, table_name):
        if not table_name.endswith('.csv'):
            table_name += '.csv'
        return table_name

    def remove_extension(self, table_name):
        if table_name.endswith('.csv'):
            table_name = table_name[:-4]
        return table_name

    def save_metadata(self):
        with open(self.metadata_file, 'w') as json_file:
            json.dump(self.tables, json_file, indent=4)

    def validate_data_type(self, value, expected_data_type):
        if expected_data_type == 'INT':
            return str(value).isdigit() or value.isdigit()
        elif expected_data_type.startswith('VARCHAR(') and expected_data_type.endswith(')'):
            length = int(expected_data_type[8:-1])
            return isinstance(value, str) and len(value) <= length
        return True

    def input_create_table(self):
        table_name = input("Enter the name of the new table (without extension, e.g., 'my_table'): ")
        if table_name.strip() == "exit":
            return None
        columns = {}
        while True:
            column_name = input("Enter a column name (or leave blank to finish): ")
            if not column_name:
                break
            data_type = input(f"Enter the data type for column '{column_name}': ")
            columns[column_name] = data_type

        primary_key = input("Enter the name of the primary key column (or leave blank if none): ")
        unique_constraints = input(
            "Enter a comma-separated list of unique constraint columns (or leave blank if none): "
            )
        if unique_constraints:
            unique_constraints = [col.strip() for col in unique_constraints.split(',')]

        foreign_keys = []
        while True:
            foreign_key = input("Enter the name of a foreign key column (or leave blank to finish): ")
            if not foreign_key:
                break
            referenced_table_column = input(
                f"Enter the referenced table and column for '{foreign_key}' (e.g., 'other_table.column'): "
                )
            foreign_keys.append({foreign_key:referenced_table_column})

        self.create_table(table_name, columns, primary_key, unique_constraints, foreign_keys)
        print(f"Table '{table_name}' has been created.")

    def input_search_query(self, query_str):
        query_str = query_str.strip()
        # Define regular expressions for extracting information
        search_pattern = re.compile(r"search\s*table\s*(\w+)(?:\s*,\s*where\s*([^,]+))?(?:\s*,\s*columns\s*=\s*\[([^\]]+)\])?(?:\s*,\s*group_by\s*=\s*(\w+))?(?:\s*,\s*sort_by\s*=\s*(\w+))?(?:\s*,\s*ascending\s*=\s*(\w+))?")
        # Extract information using regular expressions
        search_match = search_pattern.search(str(query_str))#query_str)

        # Initialize variables with default values
        table = None
        conditions = None
        columns = None
        sort_by = None
        ascending = None
        group_by = None

        # Assign values if matches are found
        # Assign values if matches are found
        if search_match:
            table, conditions_str, columns_str, group_by, sort_by, ascending_str= search_match.groups()

            # Extract conditions if present
            if conditions_str:
                conditions = [condition.strip() for condition in conditions_str.split('and')]

            # Extract columns if present
            if columns_str:
                columns = [column.strip() for column in columns_str.split(',')]

            # Convert ascending to boolean
            ascending = True if ascending_str and ascending_str.lower() == 'true' else False

        if not table:
            print("Invalid query format. Please follow the example below:")
            print(
                "Example Query: search table players where name like XYZ and goals >= 4, columns = [playerID, name], sort_by = goals, ascending = True"
                )
            return
        final_conditions = []
        if conditions!=None:
            for condition in conditions:
                final_conditions.append(condition.split(" "))
        # Perform the search
        self.search(table, conditions=final_conditions, columns=columns, sort_by=sort_by, ascending=ascending, group_by=group_by)

    def input_insert_query(self, query_str):
        query_str = query_str.strip()
        if query_str == "exit":
            return None
        pattern = re.compile(
            r"insert\s+into\s+(\w+)\s*values\s*=\s*{([^}]*)}",
            re.IGNORECASE
            )
        match = pattern.match(query_str)

        if not match:
            print("Invalid query format.")
            print("Example Query: insert into players values = {'playerID': 10000, 'name': 'XYZ'}")
            return False

        table_name = match.group(1)
        values_str = match.group(2)
        values = ast.literal_eval("{" + values_str + "}")

        return self.insert(table_name, values)

    def input_update_query(self, query_str):
        query_str = query_str.strip()
        if query_str == "exit":
            return None
        pattern = re.compile(
            r"update\s+(\w+)\s*set\s*{([^}]*)}\s*where\s*{([^}]*)}",
            re.IGNORECASE
        )
        match = pattern.match(query_str)

        if not match:
            print("Invalid update query format.")
            print("Example Query: update players set {'name': 'Ronaldo'} where {'playerID': 100000}")
            return False

        table_name = match.group(1)
        set_values_str = match.group(2)
        conditions_str = match.group(3)

        set_values = ast.literal_eval("{" + set_values_str + "}")
        conditions = ast.literal_eval("{" + conditions_str + "}")

        return self.update(table_name, conditions, set_values)

    def input_delete_query(self, query_str):
        query_str = query_str.strip()
        if query_str == "exit":
            return None
        pattern = re.compile(
            r"delete\s+(\w+)\s*where\s*{([^}]*)}(?:\s*,\s*force\s*=\s*(\w+))?",
            re.IGNORECASE
            )
        match = pattern.match(query_str)

        if not match:
            print("Invalid delete query format.")
            print("Example Query: delete players where {'playerID': '100000'}, force=True")
            return False

        table_name = match.group(1)
        conditions_str = match.group(2)
        force_str = match.group(3) if match.group(3) else False

        conditions = ast.literal_eval("{" + conditions_str + "}")
        force = force_str if force_str==False else force_str.capitalize()

        return self.delete(table_name, conditions, force)


    def input_join_query(self, query_str):
        query_str = query_str.strip()
        # Define regular expressions for extracting information
        join_pattern = re.compile(
            r"join\s+(\w+)\s+with\s+(\w+)\s+on\s*(\w+)\s*,\s*conditions\s*=\s*([^,]+)\s*,\s*columns\s*=\s*\[([^\]]*)\]\s*"
            )
        # Extract information using regular expressions
        join_match = join_pattern.search(query_str)

        # Initialize variables with default values
        table_name1 = None
        table_name2 = None
        on_column = None
        conditions = None
        columns_str = None

        # Assign values if matches are found
        if join_match:
            table_name1, table_name2, on_column, conditions_str, columns_str = join_match.groups()

            # Extract conditions if present
            # Extract conditions if present
            if conditions_str:
                conditions = [condition.strip() for condition in conditions_str.split('and')]

            # Extract columns if present
            if columns_str:
                columns = [column.strip() for column in columns_str.split(',')]

        if not table_name1 or not table_name2 or not on_column:
            print("Invalid query format. Please follow the example below:")
            print("Example Query: join 'players' with 'appearance' on 'playerID', conditions=[('goals', '>=', '4')], columns=['goals']")
            return

        final_conditions = []
        if conditions != None:
            for condition in conditions:
                final_conditions.append(condition.split(" "))
        # Perform the join
        self.join_with_condition(table_name1, table_name2, on_column, final_conditions, columns)

if __name__ == "__main__":
    db = Database()
    print("""
██╗   ██╗ ██████╗ ██╗      █████╗ ████████╗██╗██╗     ███████╗██████╗ ██████╗ 
██║   ██║██╔═══██╗██║     ██╔══██╗╚══██╔══╝██║██║     ██╔════╝██╔══██╗██╔══██╗
██║   ██║██║   ██║██║     ███████║   ██║   ██║██║     █████╗  ██║  ██║██████╔╝
╚██╗ ██╔╝██║   ██║██║     ██╔══██║   ██║   ██║██║     ██╔══╝  ██║  ██║██╔══██╗
 ╚████╔╝ ╚██████╔╝███████╗██║  ██║   ██║   ██║███████╗███████╗██████╔╝██████╔╝
  ╚═══╝   ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚══════╝╚══════╝╚═════╝ ╚═════╝                                                                                          
    """)
    def print_help():
            print("Example queries:")
            print("1. create - YOU DON'T NEED A QUERY TO CREATE A TABLE.")
            print("2. insert - Example Query: insert into players values = {'playerID': 10000, 'name': 'XYZ'}")
            print("3. search - Example Query: search table players where name like XYZ and goals >= 4, columns = [playerID, name], group_by = goals, sort_by = goals, ascending = True")
            print("4. update - Example Query: update players set {'name': 'Ronaldo'} where {'playerID': 100000}")
            print("5. delete - Example Query: delete players where {'playerID': '100000'}, force=True")
            print("6. join - Example Query: join appearance with players on  playerID, conditions=goals >= 4, columns= [name,playerID,goals] ")
            print("Type 'exit' to quit the CLI.")


    print("Welcome to VolatileDB CLI. Type 'help' for example queries or 'exit' to quit.")
    # Define the command-function mapping
    def parse_query(query):
        query = query.lower()
        # Implement a parser to determine the action from the query
        # You can use regex or other methods to identify keywords
        if 'search' in query:
            return db.input_search_query
        elif 'insert' in query:
            return db.input_insert_query
        elif 'update' in query:
            return db.input_update_query
        elif 'delete' in query:
            return db.input_delete_query
        elif 'join' in query:
            return db.input_join_query
        elif 'create' in query:
            return db.input_create_table
        else:
            return None


    while True:
        user_input = input("VolatileDB> ")

        if user_input == 'exit':
            print("Exiting VolatileDB CLI. Goodbye!")
            break

        if user_input == 'help':
            print_help()
        else:
            # Attempt to parse the query and get the corresponding function
            query_function = parse_query(user_input)

            if query_function:
                query_function(user_input)
            else:
                print("Invalid command. Type 'help' for example queries or 'exit' to quit.")

    # Example: Search for pla

    # yers who scored goals in the "appearance" table
    # Example: Search for player IDs who scored goals in the "appearance" table
    #
    # db.search('players', conditions=[('name', 'like', 'XYZ')], columns=['playerID', 'name'], sort_by='goals', ascending=True)


    # user_condition = ('goals', '>=', '4')
    # result_filename = db.join_with_condition('appearance', 'players', 'playerID', user_condition, selected_columns=['playerID', 'name', 'goals'])

    # players_metadata = {
    #     'table_name':'players.csv',
    #     'columns':{
    #         'playerID':'INT', 'name':'VARCHAR(255)'
    #         },
    #     'primary_key':'playerID',
    #     'unique_constraints':None,
    #     'foreign_keys':None
    #     }
    # db.create_table(**players_metadata)

    # teams_metadata = {
    #     'table_name':'teams.csv',
    #     'columns':{
    #         'teamID':'INT', 'name':'VARCHAR(255)'
    #         },
    #     'primary_key':'teamID',
    #     'unique_constraints':None,
    #     'foreign_keys':None
    #     }
    # db.create_table(**teams_metadata)

    # leagues_metadata = {
    #     'table_name':'leagues.csv',
    #     'columns':{
    #         'leagueID':'INT', 'name':'VARCHAR(255)', 'understatNotation':'VARCHAR(255)'
    #         },
    #     'primary_key':'leagueID',
    #     'unique_constraints':None,
    #     'foreign_keys':None
    #     }
    # db.create_table(**leagues_metadata)
    #
    # appearance_metadata = {
    #     'table_name':'appearance.csv',
    #     'columns':{
    #         'gameID':'INT', 'playerID':'INT', 'goals':'INT', 'ownGoals':'INT',
    #         'shots':'INT', 'position':'VARCHAR(255)', 'positionOrder':'INT',
    #         'yellowCard':'INT', 'redCard':'INT', 'time':'INT', 'leagueID':'INT'
    #         },
    #     'primary_key':['gameID', 'playerID'],
    #     'unique_constraints':None,
    #     'foreign_keys':[{'playerID':'players'}, {'leagueID':'leagues'}]
    #     }
    # db.create_table(**appearance_metadata)

    # employees_columns = {
    #     'employee_id': 'INT',
    #     'first_name': 'VARCHAR(50)',
    #     'last_name': 'VARCHAR(50)',
    #     'department_id': 'INT',
    # }
    #
    # departments_columns = {
    #     'department_id': 'INT',
    #     'department_name': 'VARCHAR(50)',
    # }
    #
    # db.create_table('departments', departments_columns, primary_key='department_id')
    # db.create_table('employees', employees_columns, primary_key='employee_id', unique_constraints=['first_name'],
    #                 foreign_keys=[
    #                     {'department_id': 'departments'},
    #                 ]
    #                 )
    #
    # db.insert('departments.csv', {'department_id': '101', 'department_name': 'HR'})
    # db.insert('departments.csv', {'department_id': '102', 'department_name': 'Engineering'})
    #
    # db.insert('employees.csv',
    #           {'employee_id': '1', 'first_name': 'John', 'last_name': 'Doe', 'department_id': '101'}
    #           )
    # db.insert('employees.csv',
    #           {'employee_id': '2', 'first_name': 'Jane', 'last_name': 'Smith', 'department_id': '102'}
    #           )
    #
    # db.update('employees.csv', condition={'employee_id': '1'},
    #           new_data={'first_name': 'Updated John', 'last_name': 'Updated Doe','department_id':'103'}
    #           )
    # #
    # db.delete('departments.csv', condition={'department_id': '101'})
    # #
    # db.delete('employees.csv', condition={'employee_id': '1'})

    # result = db.join('employees', 'departments', 'department_id', join_type='inner')
    # if result:
    #     print("Inner Join Results:")
    #     for row in result:
    #         print(row)
    # else:
    #     print("No records found.")

    # try:
    #         # Ensure that the temporary files are removed in case of an error
    #     temp_files = ['temp_insert_file.csv', 'temp_update_file.csv', 'temp_delete_file.csv', 'temp_delete_file_ref.csv']
    #     for temp_file in temp_files:
    #         try:
    #             if os.path.exists("./tables/"+temp_file):
    #                 os.remove("./tables/"+temp_file)
    #         except Exception as e:
    #             # Handle exceptions while trying to remove the temporary files
    #             print(f"Error while removing temporary file '{temp_file}': {str(e)}")
    # except:
    #     pass
#%%
    # conditions = [('employee_id', '>', '1')]
    # db.search('employees', conditions=conditions, columns=['employee_id','first_name','last_name','department_id'], sort_by='employee_id', ascending=False)