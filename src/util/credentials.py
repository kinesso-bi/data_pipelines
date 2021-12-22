import json
from src.util.helpers import recursive_get


def read_file(filepath: object):
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(e)
        data = filepath
    return data


def read_file_key(filepath: object, path: list):
    data = read_file(filepath)
    target_key = path[-1]
    sub_dict = recursive_get(data, *path[:-1])
    return sub_dict[target_key]


def write_file(filepath: str, data: object):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def update_file(filepath: str, new_value: object, path: list):
    data = read_file(filepath)
    target_key = path[-1]
    sub_dict = recursive_get(data, *path[:-1])
    sub_dict[target_key] = new_value
    p = path[:-1]
    for i in reversed(p):
        inner_sub_dict = recursive_get(data, *p[:-1])
        inner_sub_dict[i] = sub_dict
        sub_dict = inner_sub_dict
        p.pop()

    write_file(filepath, sub_dict)


if __name__ == "__main__":
    pass
