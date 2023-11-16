import json
from tests.functions import *


def validate_command(command: str):
    global selected_db
    command_parts = command.split()

    if command_parts[0] == 'select_db':
        if len(command_parts) != 2:
            print("Invalid command. Usage: select_db <database_name>")
        else:
            selected_db = command_parts[1]
            print(f"Selected database: {selected_db}")
            # need to check if the database exists
    elif not selected_db:
        print("No database selected. Please select a database using 'select_db <database_name>'")
    else:
        if command_parts[0] == 'insert':
            if len(command_parts) != 3:
                print("Invalid command. Usage: insert <collection_name> <data>")
            else:
                collection_name = command_parts[1]
                data = command_parts[2]
                insert_one(selected_db, collection_name, data)

        elif command_parts[0] == 'insertMany':
            if len(command_parts) != 3:
                print("Invalid command. Usage: insert_many <collection_name> <data>")
            else:
                collection_name = command_parts[1]
                data = command_parts[2]
                insert_many(selected_db, collection_name, data)

        elif command_parts[0] == 'update':
            if len(command_parts) != 4:
                print("Invalid command. Usage: update <collection_name> <condition> <data>")
            else:
                collection_name = command_parts[1]
                condition = command_parts[2]
                data = command_parts[3]
                update_one(selected_db, collection_name, condition, data)

        elif command_parts[0] == 'updateMany':
            if len(command_parts) != 4:
                print("Invalid command. Usage: update_many <collection_name> <condition> <data>")
            else:
                collection_name = command_parts[1]
                condition = command_parts[2]
                data = command_parts[3]
                update_many(selected_db, collection_name, condition, data)

        elif command_parts[0] == 'delete':
            if len(command_parts) != 3:
                print("Invalid command. Usage: delete <collection_name> <condition>")
            else:
                collection_name = command_parts[1]
                condition = command_parts[2]
                delete_one(selected_db, collection_name, condition)

        elif command_parts[0] == 'deleteMany':
            if len(command_parts) != 3:
                print("Invalid command. Usage: delete_many <collection_name> <condition>")
            else:
                collection_name = command_parts[1]
                condition = command_parts[2]
                delete_many(selected_db, collection_name, condition)

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
