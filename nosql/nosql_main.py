import jsonlines
import uvicorn
import json
import os
from fastapi import FastAPI, Path

app = FastAPI()


def read_json_lines_file(file_path):
    with jsonlines.open(file_path) as reader:
        for line in reader:
            yield line


@app.get("/{node_path:path}")
def get_data(node_path: str):
    node_list = node_path.split("/")
    try:
        if os.path.isfile(f"data/{node_list[0]}.jsonl"):
            with open(f"data/{node_list[0]}.jsonl", "r") as jsonl_file:
                if len(node_list) < 2:
                    for line in jsonl_file:
                        dict = json.loads(line)
                        yield dict
                else:  # need to find node
                    for line in jsonl_file:
                        dict = json.loads(line)
                        yield dict
        else:
            with open(f"data/{node_list[0]}_{node_list[1]}.jsonl", "r") as jsonl_file:
                for line in jsonl_file:
                    dict = json.loads(line)
                    yield dict
    except Exception as e:
        print(e)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)