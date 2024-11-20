from connections import *
from utility import ETLUtility as Utility
from supplemental_functions import *


###########################
#
# Abstract Process Controller
#
############################
class AbstractProcessController:

    def __init__(self, input_info, taskconnections=None):

        self.taskconnections = taskconnections

        self.input_info = input_info
        self.operation = input_info['task']['operation']
        self.metadata = input_info['task']['meta-data']
        self.datasource = input_info['task']['data_source']['database']
        self.datadestination = input_info['task']['data_destination']['database']
        self.pre_clean_process = input_info['task']['pre_import_process']

        self.additional_json = input_info['task']['additional_json']

        self.task_id = self.metadata['task_id']
        self.run_id = self.metadata['run_id']
        self.source = None
        self.destination = None

        ### function call_functions ###

    def controller_log(self, type, msg):

        msg = "[Task: " + str(self.task_id) + "] " + msg

        logging.log(type, msg)

    def log_chunk(self, counter):

        if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            self.controller_log(logging.DEBUG, "Get Chunk " + str(counter))
            Utility.write_to_log_table("Get Chunk: " + str(counter), 1, str(self.run_id), str(self.task_id))
        else:
            if counter % CHUNK_LOG == 0 and CHUNK_LOG > 0:
                self.controller_log(logging.INFO, "Get Chunk: " + str(counter))
                Utility.write_to_log_table("Get Chunk: " + str(counter), 1, str(self.run_id), str(self.task_id))

    def call_functions(self, df):

        for i in self.pre_clean_process:
            params = i['parameters']

            df = globals()[i['process']](df, **params)  # pass in the data frame for the process to work on
        return (df)

    # The additional_json field is  used using a key called sql_fix_processto modify the source query to allow for changes to
    # specific fields liek {{date}}, this can be used for other things, but right now it only does one.
    # so we may need to alter this later.

    def fix_sql(self, source_query):

        if 'sql_fix_processes' in self.additional_json:
            fix_sql_source_query_process = self.additional_json['sql_fix_processes']
            for p in fix_sql_source_query_process:
                key = p['key']
                function = p['function']
                parameters = p['parameters']
                if 'destination_connection' in parameters:
                    parameters['destination_connection'] = self.destination
                if 'run_id' in parameters:
                    parameters['run_id'] = self.run_id

                params = {'query': source_query, 'str_key': key, 'parameters': parameters}
                source_query = getattr(Utility, function)(**params)

            return source_query

