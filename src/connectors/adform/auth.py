import requests
from src.util.credentials import read_file_key, update_file
from src.util.helpers import wait_until, branch_operator

client_storage_filepath = ""
auth_storage_filepath = ""
token_dict_path = []


def read_token(filepath: str, dict_path: list):
    token = read_file_key(
        filepath=filepath,
        path=dict_path
    )

    return token


def create_token(client_id: str, client_secret: str):
    dict_path = "access_token"
    token_url = "https://id.adform.com/sts/connect/token"
    token_data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.adform.com/scope/buyer.stats",
        }

    response = requests.post(
        url=token_url,
        data=token_data,
    )

    return response.json()[dict_path]


def update_token(filepath: str, dict_path: list, access_token: object):
    update_file(filepath, access_token, dict_path)
    return


def post_operation(filepath: str, dict_path: list, client_id: str, client_secret: str, scope: list):
    access_token = read_token(
        filepath=filepath,
        dict_path=dict_path)
    body = {
        "dimensions": None,
        "metrics": None,
        "filter": None,
    }

    base_url = ""
    headers = {}
    json = {}

    response = requests.post(
        base_url,
        headers=headers,
        json=json
    )

    if response.status_code != 202:
        wait_until(10)
        access_token = create_token(
            client_id=client_id,
            client_secret=client_secret
        )
        update_token(
            filepath=filepath,
            dict_path=dict_path,
            access_token=access_token
        )
        post_operation(
            filepath=filepath,
            dict_path=dict_path,
            client_id=client_id,
            client_secret=client_secret
        )
    else:
        location = response.headers['Location']
        operation = response.headers['Operation-Location']
        while not branch_operator(
            input_function=read_operation_status(
                filepath=filepath,
                scope=scope,
                operation=operation
            ),
            condition=200
        ):
            wait_until(20)
        # TODO invoke function uploading data after reading location
        while not branch_operator(
            input_function=read_location(
                filepath=filepath,
                scope=scope,
                location=location
            ),
            condition=200
        ):
            wait_until(20)
    return


def read_operation_status(filepath, scope, operation):
    access_token = read_token(
        filepath=filepath,
        dict_path=scope
    )

    base_url = "https://api.adform.com/{}".format(operation)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    response = requests.get(
        url=base_url,
        headers=headers
    )

    return response.status_code


def read_location(filepath: str, scope: list, location: str):
    access_token = read_token(
        filepath=filepath,
        dict_path=scope
    )

    base_url = "https://api.adform.com/{}".format(location)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    response = requests.get(
        url=base_url,
        headers=headers
    )

    return response.status_code != 200


def read_location_data(filepath: str, scope: list, location: str) -> dict:
    access_token = read_token(
        filepath=filepath,
        dict_path=scope
    )
    base_url = "https://api.adform.com/{}".format(location)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    response = requests.get(
        url=base_url,
        headers=headers
    )

    return response.json()


def main(filepath: str, scope: str) -> object:
    client_id = read_file_key(
        filepath=filepath,
        path=[scope, "client_id"]
    )
    client_secret = read_file_key(
        filepath=filepath,
        path=[scope, "client_secret"]
    )
    # TODO get report (client_id, client_secret, scope)
    return


if __name__ == "__main__":
    pass
