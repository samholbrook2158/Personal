import os
import json
import logging

class MemphiAddin():
    """
    The MemphiAddin class is to be used to extend other memphi addins to ensure a standard approach to creating specific
    python functions. This class should contain a set of utilities to help make creating python processes more seamless and help
    speed up development time.
    ...

    Attributes
    ----------
    addin_name : str
        a formatted string that is used as the key for the configuration file. Should not be accessed directly there are corresponding setter and getter methods.
    SYS : dict
        a configfuration object with the configuration of the etl system (this is not the addins_conifgiuration
        refer to the documentation on ETL's coniguration
    CONFIG : dict
        contains the memphi_addins configuration for the given addin_name provided

    Methods
    -------
    get_sys_config(self, **kwargs)

         Gets the System Conifguration and puts the dictionary into self.SYS

         kwargs parameters:
         ------------------
         filename : refers to the file name of the configuration, defaults to config.json
                    and uses the current working directory +"/config/" for the location

         Returns:
         --------
         No return value is provided


    get_key(self, str: k, dict: d, obj: default)

         Gets a key from the given dictionary that is passed in. Used for argument passing without
         changing the function header

         Parameters:
         ----------

         k:         key to get from the dictionary

         d:         dictionary to examine

         default:   the default value to be assigned if the key (k) is not found in the dictionary (d)

         Returns:
         --------
         Returns the value imn the key or the default value if the key is not found in the dictionary

   set_addin_name(self, str: name = None):

         Sets the addin name for the process

         Parameters:
         -----------

         name:   sets the name of the module to be used in identifying the key in the addin config.
                 default if not provided is None

         Returns:
         --------

         No return value is provided

   get_config(self, **kwargs):

         Gets the addin configuration object from the configuration file and stores it in self.CONFIG
         If no conifguration file is provided the system will attempt to use the environment variable MEMPHI_ADDINS_CONFIG_FILE to find the file


         kwargs parameters:
         ------------------

         config_file: the name of the conifguration file, defaults to None if not provided

         key:         the addin key to retrieve from the configuration object. If not provided, it defaults to the
                      addin_name stored in the class by the set_addin name


         Returns
         -------

         The function will return the conifguration object even though it is stored in the self.CONFIG. This return value can be ignored



   decrypt_password(self, **kwargs):


       Decrypts a given password using the keys stored in the environment variables

       kwargs parameters:
       ------------------

       pwd:  The encrypted password to decrypt


       Returns:
       --------
       Plaintext password decrypted from the key in the environment



   get_dburl(self, **kwargs):

       Creates a URL from the string found in the CONFIG object identified by the passed in parameter.
       if the string contains {{pwd}}, the function will decrypt a separate key "ENC_PWD" and replace the {{pwd}} with the plain text passsword

       kwargs parameters:
       ------------------

       dburl:  This paramater will represent the key to extract from the CONFIG object, which will contain the URL

       Returns:
       --------

       the full dburl


   db_engine(self, **kwargs):


       Creates the sqlaclhemy engine from a given url

       kwargs parameters:
       ------------------

       dburl: the full url to create the engine from

       Returns:
       --------

       A sqlaclhemy engine object



   def send_alert(self, **kwargs):

        Sends an alert based on the configuration of the etl system


        kwargs parameters:
        ------------------

        msg: Message to be sent out

        Returns:
        --------

        1 if successful


    ==============================================================
    """


    def __init__(self):
        self.addin_name = None
        self.SYS = None
        #self.get_sys_config()


    def get_sys_config(self, **kwargs):
        filename = self.get_key("filename", kwargs, "config.json")

        curdir = os.getcwd()

        configdir = curdir + "/config/"

        configfile = configdir + filename

        # Now that we have the file lets read it.
        with open(configfile, "r") as f:
            config = json.load(f)

        self.SYS = config


    def get_key(self, k, d, default=None):
        if k in d:
            return d[k]

        return default


    def set_addin_name(self, name=None):
        self.addin_name = name


    def get_addin_name(self):
        return self.addin_name


    def get_config(self, **kwargs):
        logging.info(f"Get Config for {self.get_addin_name()}")

        config_file = self.get_key("config_file", kwargs, None)  # note change the default to a system environment variable
        addin_key = self.get_key("key", kwargs, self.get_addin_name())

        logging.debug(config_file)
        logging.debug(addin_key)

        if config_file is None:
            if 'MEMPHI_ADDINS_CONFIG_FILE' in os.environ:
                config_file = os.environ['MEMPHI_ADDINS_CONFIG_FILE']
            else:
                logging.error("No Config File Provided")
                return -1  # or raise an exception - consider creating our own exceptions?

        if addin_key is None:
            logging.error("Key Not Provided for Config")
            return -1  # or raise an exception - consider creating our own exceptions?

        if not os.path.isfile(config_file):
            logging.error("Config File Doesnt Exist")
            return -1

        try:
            with open(config_file, "r") as f:
                logging.debug(config_file)
                config = json.load(f)
        except Exception as e:
            logging.error(e) #Kara
            logging.error("Exception in Loading Config Json")
            return -1  # or raise an exception - consider creating our own excpetion

        if addin_key not in config:
            logging.error(f"Key not found in Config Dictionary {self.get_addin_name()}")
            return -1  # or raise an exception - consider creating our own excpetion

        config_key = config[addin_key]

        logging.info("Config Key Retrieved")

        self.CONFIG = config_key  # this might be better than returning it
        return config_key  # we can do additional checks or even send an alert or update the log file



    def decrypt_password(self, **kwargs):
        logging.debug("Decrypting password")
        encrypt_text = self.get_key("pwd", kwargs, None)

        if encrypt_text is None:
            logging.error("No Encrypted Text Found")
            return -1

        return py_get_decrypted_password(encrypt_text)


    def get_dburl(self, **kwargs):
        dburl_key = self.get_key("dburl", kwargs, "DBURL")

        if dburl_key not in self.CONFIG:
            logging.debug("DBURL not found in CONFIG")
            return -1  # or consider an exception ?

        dburl = self.CONFIG[dburl_key]

        if "ENC_PWD" in self.CONFIG:
            pwd = self.decrypt_password(pwd=self.CONFIG["ENC_PWD"])
            dburl = dburl.replace("{{pwd}}", pwd)

        return dburl


    def db_engine(self, **kwargs):
        # Additional parameters can be set in here and passed to the create engine

        logging.info("Get DB Engine")
        dburl = self.get_key("dburl", kwargs, None)

        if dburl is None:
            logging.error("No DBURL Specified")
            return -1  # or throw exception

        e = create_engine(dburl)

        return e


    def send_alert(self, **kwargs):
        msg = self.get_key("msg", kwargs, None)

        # this might be interesting - to send an alert to a different method
        method = self.get_key("method", kwargs, self.SYS["ALERT_SYSTEM"]["METHOD"])

        if msg is None:
            logging.warning("Alert Message was blank")
            return 1

        logging.debug("Sending Alerts")

        if self.SYS["ALERT_SYSTEM"]["ACTIVE"] == "False":  # If this is true then no alerts should be sent.
            logging.warning("Alert System Inactive - No Message Sent")
            return 1

        body = ""

        # process messages

        body = "System: " + self.SYS["SYSTEM_INFO"]["SYSTEM"] + " [" + self.SYS["SYSTEM_INFO"]["IP"] + "]\n\n"

        body = body + msg + "\n"

        # Alerts are gathered, and now we can determine what to send
        logging.debug(body)
        logging.debug(self.SYS)
        try:
            r = requests.post(self.SYS["ALERT_SYSTEM"]["URL"],
                              json={"message": body, "send_to": self.SYS["ALERT_SYSTEM"]['SEND_TO'], "method": method})
            logging.info("Alert Sent")

            # even though we get an exception there is no need to return anything special - just log the error.
        except requests.exceptions.HTTPError as errh:
            logging.error("Send Alert Error: Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            logging.error("Send Alert Error: Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            logging.error("Send Alert Error: Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            logging.error("Send Alert Error: Unknown Request Exception", err)