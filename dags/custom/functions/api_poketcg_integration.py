from airflow.hooks.base import BaseHook
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


api_connection = BaseHook.get_connection('api_pokemontcg_connection')
host = api_connection.host
api_key = api_connection.password
headers = {"X-Api-Key": api_key}


def requests_retry_session(
    retries = 3,
    backoff_factor = 0.3,
    status_forcelist = (500, 502, 504),
    session = None
):
    session = session or Session()
    session.headers.update(headers)

    retry = Retry(
        total = retries,
        backoff_factor = backoff_factor,
        status_forcelist = status_forcelist
    )

    adapter = HTTPAdapter(max_retries = retry)
    session.mount('https://', adapter)

    return session


def full_request(endpoint):

    session = requests_retry_session()

    responses_array = []
    page = 1
    response = {'count': 1}

    while response['count'] > 0:
        response = session.get( f'{host}/{endpoint}', params={'page': page}).json()
        responses_array.extend(response['data'])
        page += 1

    return responses_array


def intraday_request(endpoint):
    a = 1+1
    return True