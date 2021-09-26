import requests
import singer
from singer import metrics
import backoff
import time
from tap_freshsales import tap_utils

LOGGER = singer.get_logger()

BASE_URL = "https://{}/"
PER_PAGE = 100


class Error(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class BadRequestError(Error):
    pass


class RateLimitError(Error):
    pass


class UnauthorizedError(Error):
    pass


class ForbiddenError(Error):
    pass


class BadGateway(Error):
    pass


class NotFoundError(Error):
    pass


class TooManyError(RateLimitError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": BadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": UnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": ForbiddenError,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    429: {
        "raise_exception": TooManyError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": BadGateway,
        "message": "Server received an invalid response."
    }
}


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


def _format(url, **kwargs):
    return url.format(**kwargs)


class Client(object):
    def __init__(self, config):
        self.api_key = config.get("api_key")
        self.domain = config.get("domain")
        self.config = config
        self.state = {}
        self.session = requests.Session()
        self.base_url = BASE_URL
        self.version = 'old'  # options: [ old | new ]
        # for new version of API, skip leads endpoint as it is removed
        self.owners = []

    def prepare_and_send(self, request):
        return self.session.send(self.session.prepare_request(request))

    def url(self, endpoint, query=None, **kwarg):
        path = self.base_url.format(self.domain)
        url = _join(path, endpoint)
        if query:
            url = _join(url, query)
        if kwarg:
            url = _format(url, **kwarg)
        return url

    def create_get_request(self, endpoint, **kwargs):
        return requests.Request(method="GET",
                                # url=self.url(endpoint),
                                url=endpoint,
                                **kwargs)

    @backoff.on_exception(backoff.expo,
                          (RateLimitError, BadRequestError, BadGateway),
                          max_tries=3,
                          factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        self.raise_for_error(response)
        return response.json()

    def get(self, request_kwargs, *args, **kwargs):
        req = self.create_get_request(request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)

    def backoff_fn(details):
        print("backoff ", details)
        if details["tries"] >= 4:
            print(f"sleeping")
            # max delay could be one hour
            time.sleep(900)  # 1800

    @tap_utils.ratelimit(1, 2)
    @backoff.on_exception(backoff.expo,
                          (RateLimitError, BadRequestError, BadGateway, TooManyError),
                          max_tries=5,
                          giveup=lambda e: e.response.status_code != 429,
                          on_backoff=backoff_fn)
    def request(self, method, url, params=None, payload=None):
        """
        Rate limited API requests to fetch data from
        FreshSales API
        """
        params = params or {}
        headers = {'Accept': 'application/json'}
        if 'user_agent' in self.config:
            headers['User-Agent'] = self.config['user_agent']

        if 'api_key' in self.config:
            headers['Authorization'] = 'Token token=' + self.config['api_key']

        req = requests.Request(method, url, params=params,
                               headers=headers, json=payload).prepare()
        LOGGER.info("GET {}".format(req.url))
        resp = self.session.send(req)

        # Freshsales does not have a 'Retry-After' included in reponse header for 429 status code
        # it has 'x-ratelimit-remaining' and 'x-ratelimit-reset'
        # added backoff decorator for this
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            LOGGER.info(
                "Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            return self.request(method, url, params, payload)

        self.raise_for_error(resp)
        return resp

    def gen_request(self, method, stream, url, params=None, payload=None, name=False):
        """
        Generator to yield rows of data for given stream
        1. FILTERS :: ['filters', 'meta']
        2. STREAM or ENTITY :: ['{stream}', 'meta']
        example: (without entity included in params)
                {
                    "contacts": [], # holds all entity records/ data
                    "meta": {
                        "total_pages": 0,
                        "total": 0
                    }
                }
        """

        params = params or {}
        params["per_page"] = PER_PAGE
        params["sort"] = 'updated_at'
        params["sort_type"] = 'asc'
        page = 1
        # Meta tag carries number of pages
        # Use generator to scan across all pages of output

        while True:
            params['page'] = page
            data = self.request(method, url, params, payload).json()

            data_list = []
            if type(data) == dict:
                data_list = []
                if stream in data.keys():
                    data_list = data[stream]
                if name in data.keys():
                    data_list = data[name]
                    if name == 'module_customizations':
                        # get only custom entities from list of entities in data
                        data[name] = [x for x in data[name] if x.get('custom', True)]
                        data_list = data[name]
                for row in data_list:
                    yield row

                if len(data_list) == PER_PAGE:
                    page += 1
                else:
                    break

    def get_start(self, entity):
        """
        Get bookmarked start time for specific entity
        (defined as combination of endpoint and filter)
        data before this start time is ignored
        """

        if entity not in self.state:
            self.state[entity] = self.config['start_date']
        return self.state[entity]

    def get_filters(self, endpoint):
        """
        Use Freshsales API structure to derive filters for an
        endpoint in the supported streams
        """
        url = self.url(endpoint, query='filters')
        request = self.gen_request('GET', 'filters', url)
        return list(request)

    def raise_for_error(self, resp):
        try:
            resp.raise_for_status()
        except (requests.HTTPError, requests.ConnectionError) as error:
            try:
                error_code = resp.status_code
                # Forming a response message for raising custom exception
                try:
                    response_json = resp.json()
                except Exception:
                    response_json = {}

                message = "HTTP-error-code: {}, Error: {}".format(
                    error_code,
                    response_json.get("status", ERROR_CODE_EXCEPTION_MAPPING.get(
                        error_code, {})).get("message", "Unknown Error")
                    )

                exc = ERROR_CODE_EXCEPTION_MAPPING.get(
                    error_code, {}).get("raise_exception", Error)
                raise exc(message, resp) from None

            except (ValueError, TypeError):
                raise Error(error) from None
