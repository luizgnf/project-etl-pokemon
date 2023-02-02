from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


host = 'https://pokeapi.co/api/v2/'


def requests_retry_session(
    retries = 3,
    backoff_factor = 0.3,
    status_forcelist = (500, 502, 504),
    session = None
):
    session = session or Session()

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
    next_url = f'{host}/{endpoint}'

    while next_url is not None:     
        response = session.get(next_url).json()
        next_url = response['next']

        for result in response['results']:
            result_url = result['url']
            item_response = session.get(result_url).json()        
            responses_array.append(item_response)

    return responses_array