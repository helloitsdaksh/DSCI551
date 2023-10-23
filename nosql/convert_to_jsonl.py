"""
A python script to convert JSON file to JSON Lines
If the JSON file contains parent nodes, then it breaks the data into separate JSON Lines files.
(the parent node name is used as the file name)
If it does not contain parent nodes, then it creates a single JSON Lines file
e.g.
Input file: data/sample.json
Parent node name: students, instructors, and courses
Output file: data/sample_students.jsonl, data/sample_instructors.jsonl, data/sample_courses.jsonl

NOTE: this script does not keep track of the size of the JSON Lines files.
might have to implement splitting of the JSON Lines files in the future
"""

import os
import re


def extract_parent_node_name(string):
    string = re.sub(r"[^\w\s]", "", string)
    return string


def process_json(input_file):
    input_path = os.path.splitext(input_file)[0]

    inside_parent_node = False
    inside_object = False
    has_parent_node = False
    parent_node = ' '
    current_object = ''

    with open(input_file, 'r') as json_file:
        for line in json_file:
            l = line.strip()

            if l.startswith('"') and l.endswith('['):
                parent_node = extract_parent_node_name(l)
                inside_parent_node = True
                has_parent_node = True
            elif l.startswith('[') and not inside_parent_node:
                inside_parent_node = True
            elif inside_parent_node:
                if l.startswith('{'):
                    inside_object = True
                    current_object += '{'
                elif l.startswith('"') and inside_object:
                    current_object += l
                elif l.startswith('}') and inside_object:
                    inside_object = False
                    current_object += '}\n'
                    if has_parent_node:
                        output_file = f'{input_path}_{parent_node}.jsonl'
                    else:
                        output_file = f'{input_path}.jsonl'
                    if os.path.isfile(output_file):
                        with open(output_file, 'a') as jsonl_file:
                            jsonl_file.write(current_object)
                    else:
                        with open(output_file, 'w') as jsonl_file:
                            jsonl_file.write(current_object)
                    current_object = ''

            if l.startswith(']') and inside_parent_node:
                inside_parent_node = False


if __name__ == '__main__':
    input_file = 'data/sample_product.json'
    process_json(input_file)
    print('Done!')

