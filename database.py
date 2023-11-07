import os
import csv
import json
import tempfile


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

        return True

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
                if row[column] == value:
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

        return True

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
                                if referenced_table == table_name.replace(".csv",""):
                                    if not force:
                                        print(
                                            f"Error: Records from '{table_name.replace('.csv','')}' are referenced in '{ref_table}'. Deletion is not allowed."
                                            )
                                        return

        temp_table_file_path = os.path.join(self.table_folder, 'temp_delete_file.csv')
        table_file_path = os.path.join(self.table_folder, table_name)

        delete_successful = False

        with open(table_file_path, mode='r', newline='') as csv_file, open(temp_table_file_path, mode='w', newline=''
                                                                           ) as temp_file:
            reader = csv.DictReader(csv_file)
            writer = csv.DictWriter(temp_file, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                if not all(row.get(key) == value for key, value in condition.items()):
                    writer.writerow(row)
                else:
                    delete_successful = True

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

        return delete_successful

    def join(self, table_name1, table_name2, on_column, join_type="inner"):
        table_name1 = self.add_extension(table_name1)
        table_name2 = self.add_extension(table_name2)

        if table_name1 not in self.tables or table_name2 not in self.tables:
            print("Error: One or both of the specified tables do not exist.")
            return []

        table1_metadata = self.tables[table_name1]
        table2_metadata = self.tables[table_name2]

        if on_column not in table1_metadata["columns"] or on_column not in table2_metadata["columns"]:
            print(f"Error: Column '{on_column}' does not exist in one or both of the tables.")
            return []

        result = []

        with open(os.path.join(self.table_folder, table_name1), 'r', newline='') as csv_file1, \
                open(os.path.join(self.table_folder, table_name2), 'r', newline='') as csv_file2:

            reader1 = csv.DictReader(csv_file1)
            reader2 = csv.DictReader(csv_file2)

            if join_type == "inner":
                for row1 in reader1:
                    csv_file2.seek(0)  # Reset the second CSV file to the beginning
                    for row2 in reader2:
                        if row1[on_column] == row2[on_column]:
                            result.append({**row1, **row2})

            elif join_type == "left":
                for row1 in reader1:
                    found_match = False
                    csv_file2.seek(0)  # Reset the second CSV file to the beginning
                    for row2 in reader2:
                        if row1[on_column] == row2[on_column]:
                            result.append({**row1, **row2})
                            found_match = True
                    if not found_match:
                        result.append({**row1, **{k:None for k in reader2.fieldnames}})

            elif join_type == "right":
                for row2 in reader2:
                    found_match = False
                    csv_file1.seek(0)  # Reset the first CSV file to the beginning
                    for row1 in reader1:
                        if row1[on_column] == row2[on_column]:
                            result.append({**row1, **row2})
                            found_match = True
                    if not found_match:
                        result.append({**{k:None for k in reader1.fieldnames}, **row2})

            elif join_type == "outer":
                for row1 in reader1:
                    found_match = False
                    csv_file2.seek(0)  # Reset the second CSV file to the beginning
                    for row2 in reader2:
                        if row1[on_column] == row2[on_column]:
                            result.append({**row1, **row2})
                            found_match = True
                    if not found_match:
                        result.append({**row1, **{k:None for k in reader2.fieldnames}})

                csv_file2.seek(0)  # Reset the second CSV file to the beginning
                for row2 in reader2:
                    found_match = False
                    csv_file1.seek(0)  # Reset the first CSV file to the beginning
                    for row1 in reader1:
                        if row1[on_column] == row2[on_column]:
                            found_match = True
                            break
                    if not found_match:
                        result.append({**{k:None for k in reader1.fieldnames}, **row2})

        return result

    # def search(self, table_name, condition=None, columns=None):
    #     table_name = self.add_extension(table_name)
    #
    #     if table_name not in self.tables:
    #         print(f"Error: Table '{table_name}' does not exist.")
    #         return []
    #
    #     table_metadata = self.tables[table_name]
    #     table_columns = table_metadata["columns"]
    #     headers = columns if columns else list(table_columns.keys())
    #
    #     result = []
    #     with open(os.path.join(self.table_folder, table_name), 'r', newline='') as csv_file:
    #         reader = csv.DictReader(csv_file)
    #         for row in reader:
    #             if not condition or all(row.get(key) == value for key, value in condition.items()):
    #                 result.append({key:row[key] for key in headers})
    #
    #     return result

    def search(self, table_name, conditions=None, columns=None, sort_by=None, ascending=True, join_table=None, on_column=None):
        table_name = self.add_extension(table_name)

        if table_name not in self.tables:
            print(f"Error: Table '{table_name}' does not exist.")
            return

        result_temp_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
        result_csv_writer = csv.DictWriter(result_temp_file, fieldnames=columns)
        result_csv_writer.writeheader()

        with open(os.path.join(self.table_folder, table_name), 'r', newline='') as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                if conditions is None or all(self.check_condition(row, condition) for condition in conditions):
                    result_csv_writer.writerow({key: row[key] for key in columns})

        result_temp_file.close()

        if sort_by and sort_by in columns:
            self.sort_temp_file(result_temp_file.name, sort_by, ascending, columns)

        self.print_temp_file(result_temp_file.name)  # Print the result from the temp file

        os.remove(result_temp_file.name)  # Delete the temporary file after use

    def check_condition(self, row, condition):
        key, operator, value = condition

        # Assuming the columns are stored as strings in the CSV
        if isinstance(row[key], str):
            if row[key].isdigit():
                row[key] = int(row[key])
            else:
                try:
                    row[key] = float(row[key])
                except ValueError:
                    pass  # If it's neither an int nor a float, keep it as a string

        if operator == ">":
            if not row[key] > float(value):
                return False
        elif operator == "<":
            if not row[key] < float(value):
                return False
        elif operator == "==":
            if not row[key] == float(value):
                return False
        elif operator == ">=":
            if not row[key] >= float(value):
                return False
        elif operator == "<=":
            if not row[key] <= float(value):
                return False

        return True

    def sort_temp_file(self, file_path, sort_by, ascending, columns):
        chunk_size = 1000  # Number of rows to be read at a time

        # Read the file in chunks, sort each chunk, and write back to the temporary file
        with open(file_path, 'r', newline='') as temp_file:
            reader = csv.DictReader(temp_file)
            sorted_chunks = []
            while True:
                chunk = [row for _, row in zip(range(chunk_size), reader)]
                if not chunk:
                    break
                sorted_chunk = sorted(chunk, key=lambda row: row[sort_by], reverse=not ascending)
                sorted_chunks.append(sorted_chunk)

        # Merge the sorted chunks and write them back to the temporary file
        with open(file_path, 'w', newline='') as temp_file:
            writer = csv.DictWriter(temp_file, fieldnames=columns)
            writer.writeheader()
            while sorted_chunks:
                chunk = sorted_chunks.pop(0)
                writer.writerows(chunk)

    def print_temp_file(self, file_path):
        with open(file_path, 'r', newline='') as temp_file:
            reader = csv.DictReader(temp_file)
            for row in reader:
                print(row)

    # def merge_sort(self, data, sort_by, ascending=True):
    #     data = list(data)
    #     if len(data) <= 1:
    #         return data
    #
    #     mid = len(data) // 2
    #     left = self.merge_sort(data[:mid], sort_by, ascending)
    #     right = self.merge_sort(data[mid:], sort_by, ascending)
    #
    #     return self.merge(left, right, sort_by, ascending)
    #
    # def merge(self, left, right, sort_by, ascending=True):
    #     merged = []
    #     left_index = right_index = 0
    #
    #     while left_index < len(left) and right_index < len(right):
    #         if (left[left_index][sort_by] <= right[right_index][sort_by]) == ascending:
    #             merged.append(left[left_index])
    #             left_index += 1
    #         else:
    #             merged.append(right[right_index])
    #             right_index += 1
    #
    #     merged.extend(left[left_index:])
    #     merged.extend(right[right_index])
    #     return merged

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
            return value.isdigit()
        elif expected_data_type.startswith('VARCHAR(') and expected_data_type.endswith(')'):
            length = int(expected_data_type[8:-1])
            return isinstance(value, str) and len(value) <= length
        return True

    def input_create_table(self):
        table_name = input("Enter the name of the new table (without extension, e.g., 'my_table'): ")

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

if __name__ == "__main__":
    db = Database()

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
    conditions = [('employee_id', '>', '1')]
    db.search('employees', conditions=conditions, columns=['employee_id','first_name','last_name','department_id'], sort_by='employee_id', ascending=False)