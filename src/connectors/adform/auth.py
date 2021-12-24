import requests

from src.util.credentials import read_file_key, update_file
from src.util.helpers import wait_until, branch_operator

auth_filepath = "auth.json"
auth_dict_path = ['accounts', 'cadreon']


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
    print("create_token", response.status_code)
    return response.json()[dict_path]


def update_token(filepath: str, dict_path: list, access_token: object):
    return update_file(filepath, access_token, dict_path)


def post_operation(filepath: str, dict_path: list, client_id: str, client_secret: str, body: dict):
    access_token = read_token(
        filepath=filepath,
        dict_path=dict_path)

    base_url = "https://api.adform.com/v1/buyer/stats/data"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(access_token)
    }
    json = {
        'dimensions': body['dimensions'],
        'metrics': body['metrics'],
        'filter': body['filter']
    }

    response = requests.post(
        base_url,
        headers=headers,
        json=json
    )
    print("post_operation", response.status_code)
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
        return post_operation(
            filepath=filepath,
            dict_path=dict_path,
            client_id=client_id,
            client_secret=client_secret,
            body=body
        )
    else:
        location = response.headers['Location']
        operation = response.headers['Operation-Location']
        while not branch_operator(
                input_function=read_operation_status(
                    filepath=filepath,
                    dict_path=dict_path,
                    operation=operation
                ),
                condition=200
        ):
            wait_until(20)
        # TODO invoke function uploading data after reading location
        while not branch_operator(
                input_function=read_location(
                    filepath=filepath,
                    dict_path=dict_path,
                    location=location
                ),
                condition=200
        ):
            wait_until(20)

        return read_location_data(filepath, dict_path, location)


def read_operation_status(filepath, dict_path, operation):
    access_token = read_token(
        filepath=filepath,
        dict_path=dict_path
    )

    base_url = "https://api.adform.com/{}".format(operation)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    response = requests.get(
        url=base_url,
        headers=headers
    )
    print("read_operation_status", response.status_code)
    return response.status_code


def read_location(filepath: str, dict_path: list, location: str):
    access_token = read_token(
        filepath=filepath,
        dict_path=dict_path
    )

    base_url = "https://api.adform.com/{}".format(location)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    response = requests.get(
        url=base_url,
        headers=headers
    )
    print("read_location", response.status_code)
    return response.status_code


def read_location_data(filepath: str, dict_path: list, location: str) -> dict:
    access_token = read_token(
        filepath=filepath,
        dict_path=dict_path
    )
    base_url = "https://api.adform.com/{}".format(location)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    response = requests.get(
        url=base_url,
        headers=headers
    )
    print("read_locaiton_data", response.status_code)
    return response.json()


def main(filepath: str, dict_path: list) -> object:
    client_id = read_file_key(
        filepath=filepath,
        path=['accounts', 'cadreon', 'client_id']
    )
    client_secret = read_file_key(
        filepath=filepath,
        path=['accounts', 'cadreon', 'client_secret']
    )
    body = dict()
    body['dimensions'] = ["date", "client", "campaign", "page"]
    body['metrics'] = [
        {
            "metric": "impressions",
            "specs": {"adUniqueness": "campaignUnique"}
        },
        {
            "metric": "clicks",
            "specs": {"adUniqueness": "campaignUnique"}
        },
        {
            "metric": "sysvarProductCount"
        }
    ]
    body['filter'] = {
        "date": "lastWeek"
    }
    return post_operation(
        filepath,
        ['accounts', 'cadreon', 'token'],
        client_id,
        client_secret,
        body
    )


if __name__ == "__main__":
    print(main(auth_filepath, auth_dict_path))
