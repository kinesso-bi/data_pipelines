from functools import reduce
from time import sleep


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def wait_until(seconds: int):
    sleep(seconds)


def branch_operator(input_function, condition: object):
    if input_function != condition:
        return False
    else:
        return True
