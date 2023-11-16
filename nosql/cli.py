from nosql.functions import *
from nosql.query_executor import *
import json
import re

METADATA_FILE = 'metadata.json'


def validate_command(command: str):
    global selected_db
    # Splitting only on the first two spaces to get the command and collection name
    parts = command.split(' ', 2)
    for_query = command.split(' ', 1)

    if not parts or len(parts) < 2:
        print("Invalid command. Please enter a valid command.")
        return

    command_type = parts[0]

    if command_type == 'select_db':
        if len(parts) != 2:
            print("Invalid command. Usage: select_db <database_name>")
        else:
            selected_db = parts[1]
            db_exists = False
            with open(METADATA_FILE, 'r') as file:
                metadata = json.load(file)
                for db in metadata['databases']:
                    if db['name'] == selected_db:
                        print(f"Selected database: {selected_db}")
                        db_exists = True
                        break
                if not db_exists:
                    print(f"Database '{selected_db}' does not exist.")

    elif not selected_db:
        print("No database selected. Please select a database using 'select_db <database_name>'")

    else:
        if command_type == 'insert':
            if len(parts) != 3:
                print("Invalid command. Usage: insert <collection_name> <data>")
            else:
                collection_name = parts[1]
                data = parts[2]
                res = insert_one(selected_db, collection_name, data)
                if res:
                    print("Insertion successful.")

        elif command_type == 'insertMany':
            if len(parts) != 3:
                print("Invalid command. Usage: insert_many <collection_name> <data>")
            else:
                collection_name = parts[1]
                data = parts[2]
                res = insert_many(selected_db, collection_name, data)
                if res:
                    print("Insertion successful.")

        elif command_type == 'modify':
            # Splitting the third part to get the condition and data
            condition_and_data = re.findall(r'\{.*?\}', parts[2])
            if len(condition_and_data) != 2:
                print("Invalid command. Usage: update <collection_name> <condition> <data>")
            else:
                collection_name = parts[1]
                condition = condition_and_data[0]
                data = condition_and_data[1]
                update_one(selected_db, collection_name, condition, data)

        elif command_type == 'modifyMany':
            # Splitting the third part to get the condition and data
            condition_and_data = re.findall(r'\{.*?\}', parts[2])
            if len(condition_and_data) != 2:
                print("Invalid command. Usage: update <collection_name> <condition> <data>")
            else:
                collection_name = parts[1]
                condition = condition_and_data[0]
                data = condition_and_data[1]
                update_many(selected_db, collection_name, condition, data)

        elif command_type == 'remove':
            if len(parts) != 3:
                print("Invalid command. Usage: delete <collection_name> <condition>")
            else:
                collection_name = parts[1]
                condition = parts[2]
                delete_one(selected_db, collection_name, condition)

        elif command_type == 'removeMany':
            if len(parts) != 3:
                print("Invalid command. Usage: delete_many <collection_name> <condition>")
            else:
                collection_name = parts[1]
                condition = parts[2]
                delete_many(selected_db, collection_name, condition)

        elif command_type == 'find':
            if len(for_query) != 2:
                print("Invalid command. Usage: query <query>")
            else:
                query = for_query[1]
                execute_query(selected_db, query)
        else:
            print("Invalid command. Please try again.")


def main():
    ascii_art = """
    __  __                  ____  ____ 
   / / / /___  ____  ____  / __ \/ __ )
  / /_/ / __ \/ __ \/ __ \/ / / / __  |
 / __  / /_/ / / / / /_/ / /_/ / /_/ / 
/_/ /_/\____/_/ /_/\____/_____/_____/  
    """
    print(ascii_art)
    print("Welcome to HonoDB!")
    print("Type 'help' for a list of commands.")

    while True:
        command = input("HonoDB> ").strip()
        if command == "exit":
            print("Goodbye!")
            break
        elif command == "help":
            print("exit: Exit the program")
        else:
            validate_command(command)


if __name__ == '__main__':
    selected_db = None
    main()
