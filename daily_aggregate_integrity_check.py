import pandas as pd
import seaborn as sns
import numpy as np
import logging
import urllib
from datetime import datetime
from datetime import timedelta
import time
from multiprocessing import Pool
import sys
from pathlib import Path
import sqlite3

logger = logging.getLogger('root')
FORMAT = "[%(asctime)-15s - %(process)d][%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s"
logging.basicConfig(format=FORMAT, filename='daily_report.log', filemode='w', level=logging.DEBUG)

DB_FILE_PATH = "dbu_db.sqlite"
DB_TABLE = "DBU_DAILY_AGGR_VIEW"