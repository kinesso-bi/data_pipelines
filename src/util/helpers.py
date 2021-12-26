from functools import reduce
from time import sleep
import json


def read_file(filepath: str):
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(e)
        data = filepath
    return data


def write_file(filepath: str, data: object):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def wait_until(seconds: int):
    sleep(seconds)


def branch_operator(input_function, condition: object):
    if input_function != condition:
        return False
    else:
        return True