from pathlib import Path
import pandas as pd
from datetime import datetime
from datetime import timedelta
import logging
import urllib
from hourly_aggregate import aggregate_hour_csv
import sys
import time

logger = logging.getLogger('root')
FORMAT = "[%(asctime)-15s - %(process)d][%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s"
logging.basicConfig(format=FORMAT, filename='downloader.log', filemode='w', level=logging.DEBUG)

missing_dates = []
for delta in range(1,1081):
    target_date = (datetime.today() - timedelta(days=delta))
    if(target_date.weekday()>=5):
        continue

    date_stamp = target_date.strftime('%Y-%m-%d')
    agg_file_name = "aggregated_" + date_stamp + "_BINS_XETR.csv"
    path = Path(agg_file_name)
    if not path.exists(): 
        missing_dates.append(date_stamp)
        logger.error("missing date: " + date_stamp)
    
    if delta%30:
        logger.debug("processed: " + date_stamp)

pd.DataFrame(missing_dates).to_csv("missing_dates.csv")

url_base_path = "https://s3.eu-central-1.amazonaws.com/deutsche-boerse-xetra-pds/"
file_trail = "_BINS_XETR"

for date in missing_dates:

    hour_suffix = ""

    url_base = url_base_path + date + "/"
    include_header = True

    for h in range(7,17):
        if h<10:
            hour_suffix = "0" + str(h)
        else:
            hour_suffix = str(h)

        file_name = date + file_trail + hour_suffix + ".csv"

        logger.info("downloading: %s", url_base + file_name)
        try:
            urllib.request.urlretrieve(url_base + file_name, file_name)
        except:
            logger.error("error during download: %s, %r", url_base + file_name, sys.exc_info()[0])
            break

        if path.stat().st_size < 150:
            continue
        
        aggregate_hour_csv(file_name,include_header)
        include_header = False
        time.sleep(0.5)
    