import json
from src.util.helpers import recursive_get, read_file, write_file


def read_file_key(filepath: str, path: list):
    data = read_file(filepath)
    target_key = path[-1]
    sub_dict = recursive_get(data, *path[:-1])
    return sub_dict[target_key]


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

    return write_file(filepath, sub_dict)


if __name__ == "__main__":
    pass
