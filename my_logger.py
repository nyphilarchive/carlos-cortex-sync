# my_logger.py
import logging
import os
from os.path import join, dirname
import time
from dotenv import load_dotenv

# First, grab credentials and other values from the .env file in the same folder as this script
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

logs = os.environ.get('logs', 'default')

def setup_custom_logger():
# Find the previous log file and rename it
    filepath = logs + 'cortex-updates.log'

    if os.path.exists(filepath):
        # Get the date modified value of the previous log file and format as string
        # Borrowed from here: https://www.geeksforgeeks.org/how-to-get-file-creation-and-modification-date-or-time-in-python/
        mod_time = os.path.getmtime(filepath) 
        mod_timestamp = time.ctime(mod_time)
        time_obj = time.strptime(mod_timestamp)
        time_string = time.strftime("%Y-%m-%d-%H-%M", time_obj)
        new_filepath = logs + time_string + '_cortex-updates.log'
        # Rename the last log file
        os.rename(filepath, new_filepath)

    # Set up logging (found here: https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    logfile = logs + 'cortex-updates.log'
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    return logger

logger = setup_custom_logger()
