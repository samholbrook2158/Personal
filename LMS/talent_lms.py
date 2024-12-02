import logging
import json
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import sqlalchemy as sa
from apicall import APICall, APICodes
from MemphiAddin import MemphiAddin


class TalentAPI(APICall, MemphiAddin):
    def __init__(self):
    """
    Initializes the TalentAPI class with configurations for database and API endpoints from a JSON file.

    Attributes:
        CONFIG (dict): Configuration settings loaded from the JSON file.
        base_url (str): Base URL for API requests.
        api_key (str): API key for authentication.
        page_size (int): Default page size for API pagination.
        ENDPOINTS (dict): Mapping of endpoint names to their respective API paths.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connections.
        process_params (dict): Configuration for various data-fetching processes, including table names,
                               file names, and whether to use CSV or replace existing data.
    """
        super().__init__()
        super(MemphiAddin, self).__init__()

        self.set_addin_name("TALENT")
        self.CONFIG = self.get_config(config_file='/Users/samholbrook/PyCharmProjects/LMS/addins_settings.json')
        self.base_url = self.CONFIG['BASE_URL']
        self.api_key = self.CONFIG['API_KEY']
        self.page_size = int(self.CONFIG.get("PAGE_SIZE", 250))
        self.ENDPOINTS = self.CONFIG["ENDPOINTS"]
        self.engine = sa.create_engine(
            f"mysql+pymysql://{self.CONFIG['USER']}:{self.CONFIG['PASSWORD']}@"
            f"{self.CONFIG['HOST']}:{self.CONFIG['PORT']}/{self.CONFIG['DATABASE']}"
        )

        self.process_params = {
            "get_users": {"table_name": "users", "file_name": "users_data.csv", "use_csv": False, "replace": True},
            "get_courses": {"table_name": "courses", "file_name": "courses_data.csv", "use_csv": False,
                            "replace": True},
            "get_groups": {"table_name": "groups", "file_name": "groups_data.csv", "use_csv": False, "replace": True},
            "get_branches": {"table_name": "branches", "file_name": "branches_data.csv", "use_csv": False,
                             "replace": True},
            "get_ratelimit": {"table_name": "ratelimit", "file_name": "ratelimit_data.csv", "use_csv": False,
                              "replace": True},
            "get_categories": {"table_name": "categories", "file_name": "categories_data.csv", "use_csv": False,
                               "replace": True},
            "get_registration": {"table_name": "registration", "file_name": "registration_data.csv", "use_csv": False,
                                 "replace": True},
            "get_course_fields": {"table_name": "course_fields", "file_name": "course_fields_data.csv",
                                  "use_csv": False, "replace": True}
        }

    def main_process(self, **kwargs):
    """
    Determines the process to execute and runs the corresponding data retrieval function.

    Parameters:
        kwargs (dict): Keyword arguments containing parameters for the process. Must include:
            - process (str): Name of the method to execute (e.g., 'get_users').
            - Additional parameters specific to the method being called.

    Returns:
        int: 1 if the process completes successfully, -1 otherwise.
    """
        logging.info("Starting main process")
        logging.debug(kwargs)
        process = kwargs.get("process", None)
        if not process:
            logging.error("No process specified")
            return -1

        method = getattr(self, process, None)
        if not callable(method):
            logging.error(f"{process} is not a valid process")
            return -1

        # call function to load field definition file for debugging purposes
        try:
            with open("field_descriptions.json", "r") as f:
                self.column_types = json.load(f)
        except Exception as e:
            logging.error(f"Error during main process: {e}")
            return -1

        try:
            logging.info(f"Parameters for {process}: {self.process_params[process]}")
            result = method(**kwargs)
            logging.info("Main process completed successfully")
            return result
        except Exception as e:
            logging.error(f"Error during main process: {e}")
            return -1

    def fetch_all_pages_for_endpoint(self, endpoint):
    """
    Fetches paginated data from a specified API endpoint and combines the results.

    Parameters:
        endpoint (str): The API endpoint to retrieve data from.

    Returns:
        tuple: A tuple containing:
            - list: Combined data from all pages.
            - int: Total number of pages retrieved.
            - int: Page size used for pagination.
            - int: HTTP status code of the last response.
    """
        logging.info(f"Starting fetch for endpoint: {endpoint}")
        page_number = 1
        all_data = []
        total_pages = 0
        is_rate_limit = (endpoint == self.ENDPOINTS['RATELIMIT'])

        while True:
            paginated_url = f"{self.base_url}/{endpoint}/page_size:{self.page_size},page_number:{page_number}"
            logging.debug(f"Fetching page {page_number} from: {paginated_url}")

            response = requests.post(paginated_url, auth=HTTPBasicAuth(self.api_key, ''))
            if response.status_code != APICodes.OK.value:
                logging.error(f"Error fetching data from {paginated_url}. Status code: {response.status_code}")
                break

            current_page_data = response.json()

            logging.debug(f"Data retrieved for page {page_number}:\n{json.dumps(current_page_data, indent=4)}")

            if is_rate_limit:
                if not isinstance(current_page_data, list):
                    logging.debug("Rate limit data format detected; storing raw data as-is.")
                    all_data.append(current_page_data)
                    break
            else:
                # Add current page data to the collective list
                all_data.extend(current_page_data)
                if len(current_page_data) < self.page_size:
                    logging.debug("No more pages.")
                    break
                page_number += 1  # Move onto next page

            total_pages += 1  # Count pages processed

        logging.info(f"Total pages fetched: {total_pages}")
        return all_data, total_pages, self.page_size, response.status_code

    def insert_data(self, df: pd.DataFrame, table_name: str, use_csv: bool = False, file_name: str = None, replace: bool = True):
    """
    Inserts data from a Pandas DataFrame into a MySQL table or saves it to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data to insert.
        table_name (str): Name of the target MySQL table.
        use_csv (bool): If True, saves the data to a CSV file instead of the database. Defaults to False.
        file_name (str): Name of the CSV file to save the data to (required if use_csv is True).
        replace (bool): If True, truncates the table before inserting new data. Defaults to True.

    Returns:
        int: 1 if the insertion is all good, -1 otherwise.
    """
        logging.info(f"Inserting data into {table_name}")
        clean_table_name = table_name.replace("`", "")

        # Validate the DataFrame
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                logging.error(f"Column '{col}' contains nested dictionary data. Flatten before inserting.")
                logging.info(f"Final DataFrame columns: {df.columns}")
                logging.info(f"Final DataFrame sample: {df.head()}")
                return -1

        # Ensure DataFrame columns match the table schema
        if table_name in self.column_types:
            data_types = self.column_types[table_name]
            for col, dtype in zip(df.columns, data_types):
                try:
                    if "int" in dtype.lower():
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    elif "datetime" in dtype.lower():
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif "varchar" in dtype.lower():
                        df[col] = df[col].astype(str)
                except Exception as e:
                    logging.error(f"Error converting column '{col}' to type '{dtype}': {e}")

        # Insert the DataFrame into the database
        try:
            if use_csv:
                df.to_csv(file_name, index=False)
                logging.info(f"{len(df)} records saved to {file_name}")
            else:
                if replace:
                    with self.engine.begin() as conn:
                        conn.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
                df.to_sql(name=clean_table_name, con=self.engine, index=False, if_exists="append")
                logging.info(f"{len(df)} records inserted into {table_name}")
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")
            return -1
        return 1

    # General data fetching and inserting methods
    def get_courses(self, **kwargs):
    """
    Fetches course data from the 'courses' API endpoint, converts it into a DataFrame, and inserts it into a database
    or saves it as a CSV file.

    Parameters:
        kwargs (dict): Additional parameters, including:
            - table_name (str): Name of the target MySQL table.
            - file_name (str): Name of the CSV file (if use_csv is True).
            - use_csv (bool): Whether to save data to a CSV file. Defaults to False.
            - replace (bool): Whether to replace existing data in the database. Defaults to True.

    Returns:
        int: 1 if the process succeeds, -1 otherwise.
    """
        logging.debug("get_users")
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        # params = kwargs.get("get_users", {})
        # params = self.process_params["get_users"]
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['USERS'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch users data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_courses(self, **kwargs):
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['COURSES'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch courses data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_groups(self, **kwargs):
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['GROUPS'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch groups data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_branches(self, **kwargs):
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['BRANCHES'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch branches data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_ratelimit(self, **kwargs):
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['RATELIMIT'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch rate limit data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_categories(self, **kwargs):
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['CATEGORIES'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch categories data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_registration(self, **kwargs):
    """
    This requires extra features since the structure of this json has extra nested fields
    which pandas doesnt like at all.
    
    Fetches and processes registration data from the 'registration' API endpoint.

    This method handles nested data structures by:
    - Flattening fields to ensure compatibility with the pandas DataFrame format.
    - Resolving branch-related fields:
        - Maps branch keys to branch names using a dictionary (`branch_name`).
        - Converts branch lists into comma-separated strings.
    - Ensuring all columns in the final DataFrame contain flat, scalar values.

    After processing, the method either inserts the data into a database or saves it as a CSV file.

    Parameters:
        kwargs (dict): Keyword arguments, including:
            - table_name (str): Name of the MySQL table to insert data into (required if `use_csv` is False).
            - file_name (str): Name of the CSV file to save data to (required if `use_csv` is True).
            - use_csv (bool): Whether to save data to a CSV file. Defaults to False.
            - replace (bool): Whether to replace existing data in the database or CSV file. Defaults to True.

    Example:
        >>> t = TalentAPI()
        >>> t.get_registration(
                table_name="registration",
                file_name="registration_data.csv",
                use_csv=False,
                replace=True
            )

    Notes:
        - Logs both the raw and processed `fields` data for debugging purposes.
        - Validates the DataFrame to ensure there are no nested or non-scalar values.
        - Handles cases where 'branch_name' or 'branch' fields are missing or malformed.

    Raises:
        - Logs errors during API calls, data flattening, or database/CSV operations.
    """
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)

        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1

        # Fetch data from API
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['REGISTRATION'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch registration data")
            return -1

        # Flatten nested data before creating the DataFrame
        flattened_data = []
        for record in data:
            # Flatten 'fields' column
            if 'fields' in record:
                fields = record.pop('fields', {})
                logging.debug(f"Raw fields data: {fields}")

                # Initialize branch_name to None by default
                branch_name_dict = fields.pop('branch_name', None) or {}
                branch_keys = fields.get('branch', [])
                if isinstance(branch_keys, list) and branch_keys:
                    # Map branch keys to names in branch_name_dict
                    fields['branch_name'] = ', '.join(
                        [branch_name_dict.get(key, '') for key in branch_keys if key in branch_name_dict]
                    )
                elif isinstance(branch_keys, str):
                    # Handle single branch key as a string
                    fields['branch_name'] = branch_name_dict.get(branch_keys, '')
                else:
                    fields['branch_name'] = None

                # Flatten 'branch' into a comma-separated string if it's a list
                if 'branch' in fields:
                    fields['branch'] = ','.join(fields['branch']) if isinstance(fields['branch'], list) else fields[
                        'branch']

                # Add flattened fields back to the record
                logging.debug(f"Flattened fields data: {fields}")
                record.update(fields)

            # Append the processed record to the flattened data list
            flattened_data.append(record)

        # Create a DataFrame with flattened data
        df = pd.DataFrame(flattened_data)

        # Ensure no nested data remains in any column
        if 'branch_name' in df.columns:
            df['branch_name'] = df['branch_name'].astype(str)

        logging.info(f"Flattened registration DataFrame: {df.head()}")
        logging.info(f"DataFrame columns: {df.columns}")

        # Pass the DataFrame to the insert_data method
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def get_course_fields(self, **kwargs):
        logging.debug(kwargs)
        table_name = kwargs.get("table_name", None)
        file_name = kwargs.get("file_name", None)
        use_csv = kwargs.get("use_csv", False)
        replace = kwargs.get("replace", True)
        if use_csv == False and table_name is None:
            logging.error("No table name given for process")
            return -1
        if use_csv == True and file_name is None:
            logging.error("No file given for process")
            return -1
        data, _, _, status_code = self.fetch_all_pages_for_endpoint(self.ENDPOINTS['COURSE_FIELDS'])
        if status_code != APICodes.OK.value:
            logging.error("Failed to fetch course fields data")
            return -1
        df = pd.DataFrame(data)
        return self.insert_data(df, table_name=table_name, use_csv=use_csv, file_name=file_name, replace=replace)

    def fetch_single_item(self, endpoint_key, item_id, id_placeholder):
    """
    Fetches a single item from the specified endpoint.

    Parameters:
        endpoint_key (str): Key of the endpoint in the `ENDPOINTS` configuration.
        item_id (int or str): ID of the item to fetch.
        id_placeholder (str): Placeholder in the endpoint URL to be replaced with the item ID.

    Returns:
        dict: A dictionary containing the item's data, or None if the request fails.
    """
        logging.info(f"Fetching data for {endpoint_key} ID: {item_id}")

        # Construct the URL with the provided item ID
        url = f"{self.base_url}/{self.ENDPOINTS[endpoint_key].replace(id_placeholder, str(item_id))}"
        response = requests.get(url, auth=HTTPBasicAuth(self.api_key, ''))

        if response.status_code == 200:
            try:
                item_data = response.json()
                main_fields = {key: item_data[key] for key in item_data if not isinstance(item_data[key], list)}
                logging.info(f"Data Retrieved for {endpoint_key} ID {item_id}")
                logging.debug(json.dumps(main_fields, indent=4))
                return main_fields
            except ValueError as e:
                logging.error(f"Failed to parse JSON response for {endpoint_key} ID {item_id}: {e}")
                return None
        else:
            logging.error(
                f"Failed to retrieve data for {endpoint_key} ID {item_id}. Status Code: {response.status_code}")
            try:
                error_details = response.json()
                logging.error(f"Error Details:\n{json.dumps(error_details, indent=4)}")
            except ValueError:
                logging.error("Non-JSON response content received.")
            return None

    # Singular gets
    def get_user_from_id(self, user_id):
        data, status_code = self.fetch_single_item("GET_USER", user_id, "{userId}")
        if status_code != APICodes.OK:
            return -1
        return data

    def get_course_from_id(self, course_id):
        data, status_code = self.fetch_single_item("GET_COURSES", course_id, "{courseId}")
        if status_code != APICodes.OK:
            return -1
        return data

    def get_category_from_id(self, category_id):
        data, status_code = self.fetch_single_item("GET_CATEGORIES", category_id, "{categoryId}")
        if status_code != APICodes.OK:
            return -1
        return data

    def get_group_from_id(self, group_id):
        data, status_code = self.fetch_single_item("GET_GROUPS", group_id, "{groupId}")
        if status_code != APICodes.OK:
            return -1
        return data

    def get_branch_from_id(self, branch_id):
        data, status_code = self.fetch_single_item("GET_BRANCHES", branch_id, "{branchId}")
        if status_code != APICodes.OK:
            return -1
        return data


if __name__ == "__main__":
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    t = TalentAPI()

    additional_json = [
        {"process": "get_users",
         "table_name": "users",
         "file_name": "users_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_courses",
         "table_name": "courses",
         "file_name": "courses_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_groups",
         "table_name": "`groups`",
         "file_name": "groups_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_branches",
         "table_name": "branches",
         "file_name": "branches_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_ratelimit",
         "table_name": "ratelimit",
         "file_name": "ratelimit_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_categories",
         "table_name": "categories",
         "file_name": "categories_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_registration",
         "table_name": "registration",
         "file_name": "registration_data.csv",
         "use_csv": False,
         "replace": True},

        {"process": "get_course_fields",
         "table_name": "course_fields",
         "file_name": "course_fields_data.csv",
         "use_csv": False,
         "replace": True},
    ]

    for table_config in additional_json:
        additional_json = table_config  # Dynamically assign the current table configuration to `additional_json`
        logging.info(f"Processing table: {additional_json['table_name']}")
        result = t.main_process(**additional_json)  # Pass the updated `additional_json`
        if result == -1:
            logging.error(f"Failed to process table: {additional_json['table_name']}")
        else:
            logging.info(f"Successfully processed table: {additional_json['table_name']}")

    # Single fetch calls, might used later down the road
    # t.main_process(process="get_user_from_id", user_id=173)
    # t.main_process(process="get_course_from_id", course_id=135)
    # t.main_process(process="get_group_from_id", group_id=26)
    # t.main_process(process="get_branch_from_id", branch_id=5)
    # t.main_process(process="get_category_from_id", category_id=6)
