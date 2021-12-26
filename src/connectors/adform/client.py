import requests
from src.util.credentials import read_file_key, update_file
from src.util.helpers import wait_until, branch_operator, write_file


class Adform:
    def __init__(self, filepath: str, account: str, body_dimensions: list, body_metrics: list, body_filter: dict):
        self.filepath = filepath
        self.account = account
        self.id_path = ['accounts', account, 'client_id']
        self.secret_path = ['accounts', account, 'client_secret']
        self.token_path = ['accounts', account, 'token']
        self.access_token = None
        self.body = dict()
        self.body['dimensions'] = body_dimensions
        self.body['metrics'] = body_metrics
        self.body['filter'] = body_filter
        self.client_id = read_file_key(
            filepath=filepath,
            path=self.id_path
        )
        self.client_secret = read_file_key(
            filepath=self.filepath,
            path=self.secret_path
        )
        self.operation = None
        self.location = None

    def read_token(self):
        self.access_token = read_file_key(
            filepath=self.filepath,
            path=self.token_path
        )

    def create_token(self):
        token_url = "https://id.adform.com/sts/connect/token"
        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://api.adform.com/scope/buyer.stats",
        }
        response = requests.post(
            url=token_url,
            data=token_data,
        )
        self.access_token = response.json()["access_token"]

    def update_token(self):
        return update_file(self.filepath, self.access_token, self.token_path)

    def read_operation_status(self):
        base_url = "https://api.adform.com/{}".format(self.operation)
        headers = {"Authorization": "Bearer {}".format(self.access_token)}

        response = requests.get(
            url=base_url,
            headers=headers
        )
        return response.status_code

    def read_location(self):
        base_url = "https://api.adform.com/{}".format(self.location)
        headers = {"Authorization": "Bearer {}".format(self.access_token)}

        response = requests.get(
            url=base_url,
            headers=headers
        )
        return response.status_code

    def read_location_data(self) -> dict:
        base_url = "https://api.adform.com/{}".format(self.location)
        headers = {"Authorization": "Bearer {}".format(self.access_token)}

        response = requests.get(
            url=base_url,
            headers=headers
        )
        return response.json()

    def post_operation(self):
        self.read_token()
        base_url = "https://api.adform.com/v1/buyer/stats/data"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.access_token)
        }
        json = {
            'dimensions': self.body['dimensions'],
            'metrics': self.body['metrics'],
            'filter': self.body['filter']
        }

        response = requests.post(
            base_url,
            headers=headers,
            json=json
        )

        if response.status_code != 202:
            wait_until(10)
            self.create_token()
            self.update_token()
            return self.post_operation()
        else:
            self.location = response.headers['Location']
            self.operation = response.headers['Operation-Location']
            while not branch_operator(
                    input_function=self.read_operation_status(),
                    condition=200
            ):
                wait_until(20)

            while not branch_operator(
                    input_function=self.read_location(),
                    condition=200
            ):
                wait_until(20)

            return self.read_location_data()

    def get_report_data(self):
        return self.post_operation()
    # TODO function transforming data to csv or dataframe


def main(filepath: str, account: str) -> object:
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
        "date": "thisYear"
    }

    adform = Adform(filepath, account, body['dimensions'], body['metrics'], body['filter'])
    return adform.post_operation()


if __name__ == "__main__":
    auth_filepath = "auth.json"
    pass
