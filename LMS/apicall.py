import requests
import validators
from enum import Enum
from requests.auth import HTTPBasicAuth
import logging


class APICodes(Enum):
    NO_CODE = 0  # No Code Provided
    OK = 200  # Request succeeded
    CREATED = 201  # Resource created
    ACCEPTED = 202  # Request accepted for processing
    NO_CONTENT = 204  # No content to return
    PARTIAL_CONTENT = 206 # partial content

    # 3xx Redirection
    MOVED_PERMANENTLY = 301  # Resource moved permanently
    FOUND = 302  # Resource temporarily moved
    NOT_MODIFIED = 304  # Resource not modified (cached version can be used)

    # 4xx Client Errors
    BAD_REQUEST = 400  # Malformed request or validation error
    UNAUTHORIZED = 401  # Authentication required
    FORBIDDEN = 403  # Request forbidden despite authentication
    NOT_FOUND = 404  # Resource not found
    METHOD_NOT_ALLOWED = 405  # HTTP method not allowed on this resource
    CONFLICT = 409  # Conflict in request (e.g., resource already exists)
    UNPROCESSABLE_ENTITY = 422  # Well-formed request but invalid data (validation errors)

    # 5xx Server Errors
    INTERNAL_SERVER_ERROR = 500  # Generic server error
    BAD_GATEWAY = 502  # Invalid response from upstream server
    SERVICE_UNAVAILABLE = 503  # Server temporarily unavailable
    GATEWAY_TIMEOUT = 504  # No response from upstream server in time

class APIReturnTypes(Enum):

    TEXT = 0
    JSON = 1

class APICallException(Exception):
    pass

class APICall():
    """
    Generic API Call. The call is made using the concatenation of a base_url and endpoint.
    The Method of the call is in the call_api function and is defaulted to GET

    The response object has a number of parameters embedded in it:
            status_code
            text
            json()
            content
            headers
            encoding
            elapsed
            url
            cookies
            ok
            reason
            raise_for_status()
            history
            is_redirect
            is_permanent_redirect

        :Instance Variables:
        :response - the response object
        :response_code - response code of the call
        :return_data - dat returned from the api (as either json or string)
        :pagination - if the dictionary is not empty it will have two keys , next and prev. Next and prev will be
                      the next link for pagination and previous link for pagination

        : THINGS TO DO :
        :Retries and backoff to handle transient failures.
        :Rate limiting checks for proper API usage. - This should be
        :Error handling based on status codes. - This should probably be handled in the subclass
        :Logging for tracking API requests and errors - This should probably be handled in the sublcass
        :Authentication and caching if applicable.
        """

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self.base_url = self.get_key("base_url", kwargs, None)
        self.headers = {}
        self.response = None
        self.status_code = APICodes.NO_CODE
        self.return_data = None
        self.return_data_type = None
        self.pagination = None
        self.has_pagination = False
        self.alternate_pagination_keys = []
        self.auth = None

        # These are variables that shouldn't be used because they begin with an underscore
        # but there is no way to enforce
        self._timeout = 30   # 30 second timeout can be overriden

    def get_key(self, k, d, default = None):
        if k in d:
            return d[k]
        else:
            return default

    # Since we might use this in other areas - should we put this in a higher class or utility class?

    def is_valid_url(self, url: str) -> bool :
        """Checks if the provided URL is valid using the validators library."""
        return validators.url(url)

    def check_timeout(self, timeout) -> int:
        if isinstance(timeout, int):
                return timeout
        else:
            return self._timeout


    def set_base_url(self, baseurl) ->None :
        if baseurl is None:
            raise ValueError("No base url provided")  # Consider sending out an error

        if self.is_valid_url(baseurl):
            self.base_url = baseurl   # Should we provide a check here.
        else:
            raise ValueError("Invalid Base URL")

    def set_alternate_pagination_keys(self, alternate_keys: list= [] ) -> None:

        if isinstance(alternate_keys, list):
            self.alternate_pagination_keys = alternate_keys
        elif isinstance(alternate_keys, str):
            self.alternate_pagination_keys = alternate_keys.split(',')
        else:
            # Produce a warning here because its not a string
            self.alternate_pagination_keys = []


    def set_header(self, key=None, value=None, headers_dict=None):
        """Sets headers either using a dictionary or a single key-value pair."""
        if headers_dict:
            # If a dictionary is provided
            if not self.headers:
                # If self.headers is empty, set it to the new dictionary
                self.headers = headers_dict
            else:
                # If self.headers is not empty, update it with new keys or overwrite existing ones
                self.headers.update(headers_dict)
        elif key and value:
            # If a key and value are provided, add/update the single header
            self.headers[key] = value
        else:
            raise APICallException("Invalid Header Assignment")

    def call_api(self, baseurl = None, endpoint = None, method="GET", data=None, params=None, timeout = None):
        """Performs an API call to a specific endpoint."""

        #Check Timeout as best as possible
        if timeout is not None:
            timeout = self.check_timeout(timeout)
        else:
            timeout = self._timeout

        if baseurl is None:
            url = f"{self.base_url}/{endpoint}"
        else:
            url = baseurl
        print(url)
        print(self.headers)
        try:
            print(self.auth)
            response = requests.request(
                method, url, headers=self.headers, json=data, params=params, timeout = timeout, auth = self.auth
            )
            self.response = response
            self.status_code = response.status_code

            self.return_data =  self.get_response(response)
            if isinstance(self.return_data, dict):
                self.return_data_type = APIReturnTypes.JSON
            else:
                self.return_data_type = APIReturnTypes.TEXT

            # Check if pagination is present
            # Alternate pagination keys are here in case the API call
            # doesnt use standard keys for pagination
            self.has_pagination = self.check_pagination(self.return_data, self.alternate_pagination_keys)

        except requests.exceptions.Timeout:
            raise APICallException("Request timed out.")

        except requests.exceptions.RequestException as e:
            #logging.error(.....)
            raise APICallException(f"error {e}")

    def get_response(self, response):
        """Processes the API response and returns it in JSON or text format."""
        try:
            return response.json()
        except ValueError:
            return response.text

    def check_pagination(self, data, alternate_keys: list = [] )  ->bool:
        """
        Checks if the API response contains pagination.
        This example assumes common pagination keys, but modify according to your API's specification.
        """
        if not isinstance(alternate_keys, list):
            # Set a warning that the alternate keys is not a list
            try:
                alternate_keys = [ str(alternate_keys) ]
            except Exception as e:
                # Again set a warning that the alternate keys conversion failed.
                # This means it couldnt create the list from alternate keys so
                # we will just create an empty list and proceed forward.
                alternate_keys = []
        # Common pagination keys, adjust according to your API
        pagination_keys = ['next', 'previous', 'page', 'per_page', 'total_pages', 'links']
        pagination_keys.extend(alternate_keys)
        pagination_keys = list(set(pagination_keys))  # This will make sure its unique

        if isinstance(data, dict):
            self.pagination = {key: data[key] for key in pagination_keys if key in data}
            return bool(self.pagination)

        # If no pagination is detected
        return False

    def set_basic_auth(self, key):
        self.auth = HTTPBasicAuth(key, '')
        logging.debug('set_basic_auth')