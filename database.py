class Database:
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name, columns):
        """
        Creates a new table with the given table name and columns.

        Args:
            table_name (str): The name of the table to be created.
            columns (list): A list of column names for the table.

        Returns:
            str: A message indicating the success or failure of the table creation.
        """
        if table_name not in self.tables:
            self.tables[table_name] = {'columns': columns, 'data': []}
            return f"Table '{table_name}' created with columns: {', '.join(columns)}"
        else:
            return f"Table '{table_name}' already exists."

    def insert_into(self, table_name, values):
        """
        Inserts values into a specified table.

        Parameters:
            table_name (str): The name of the table to insert values into.
            values (list): A list of values to be inserted into the table.

        Returns:
            str: A message indicating the success or failure of the insert operation.
        """
        if table_name in self.tables:
            table = self.tables[table_name]
            if len(values) != len(table['columns']):
                return "Number of values does not match the number of columns."

            table['data'].append(dict(zip(table['columns'], values)))
            return f"Inserted values into '{table_name}'."

        return f"Table '{table_name}' does not exist."

    def select_from(self, table_name, conditions=None):
        """
        Selects rows from a table based on the specified conditions.

        Parameters:
            table_name (str): The name of the table to select from.
            conditions (dict, optional): The conditions to filter the rows by. Defaults to None.

        Returns:
            list: The selected rows from the table.

        Raises:
            None
        """
        if table_name in self.tables:
            table = self.tables[table_name]
            if conditions:
                results = []
                for row in table['data']:
                    if all(row[column] == value for column, value in conditions.items()):
                        results.append(row)
                return results
            else:
                return table['data']
        return []


