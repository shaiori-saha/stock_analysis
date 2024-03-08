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

days_offset = 0
logger = logging.getLogger('root')
FORMAT = "[%(asctime)-15s - %(process)d][%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s"
logging.basicConfig(format=FORMAT, filename='downloader.log', filemode='w', level=logging.DEBUG)

def simplify_time(input):
    return int(input.split(":")[1])

def aggregate_hour_csv(filename, include_header):

    df_hour = pd.read_csv(filename, index_col=False, usecols=["Mnemonic", "SecurityType", \
                                "Time", "StartPrice", "MaxPrice", "MinPrice", \
                                "EndPrice", "TradedVolume", "NumberOfTrades"])
    hour_stamp = filename[-6:-4]

    logger.debug("read file %s, no rows: %d", filename, len(df_hour))
    df_hour_common_stocks = df_hour.loc[df_hour["SecurityType"] == "Common stock"]
    logger.debug("common stock total trades: %d", len(df_hour_common_stocks))

    # get all mnemonics
    traded_stocks = df_hour_common_stocks["Mnemonic"].unique().tolist()
    logger.debug("no of common stocks traded: %d", len(traded_stocks))

    # currencies stocks traded in
    # traded_currencies = df_hour_common_stocks["Currency"].unique().tolist()
    # logger.debug("currencies common stocks traded: %r", traded_currencies)

    # for each mnemonic => start, end, cumm high/low, total volume
    grouped_traded_stocks = df_hour_common_stocks.groupby("Mnemonic")
    def func(x):
        d = [hour_stamp]
        d.append(np.min(x['MinPrice']))
        d.append(np.max(x['MaxPrice']))
        # d.append(x.loc[np.min(x['Time']) == x['Time']]["StartPrice"].item())
        # d.append(x.loc[np.max(x['Time']) == x['Time']]['EndPrice'].item())
        d.append(x.iat[0, 3].item())
        d.append(x.iat[-1, 6].item())
        d.append(x['TradedVolume'].sum())
        d.append(x['NumberOfTrades'].sum())
        return pd.Series(d, index=["Hour", "MinPrice", "MaxPrice", "StartPrice", "EndPrice", "TradedVolume", "NumTrades"])

    grouped_traded_stocks_startend = grouped_traded_stocks.apply(func)
    logger.debug("aggregated stocks : %d", len(grouped_traded_stocks))
    
    grouped_traded_stocks_startend.to_csv("aggregated_" + filename[:-6] + ".csv", mode='a', header=include_header)

    # send entry to db

    # total no of vol and transaction for that hour
    # weighted trade vol
    return

def monthly_downloader(init_delta):

    url_base_path = "https://s3.eu-central-1.amazonaws.com/deutsche-boerse-xetra-pds/"
    file_trail = "_BINS_XETR"
    hour_suffix = ""

    for i in range(30):
        target_date = (datetime.today() - timedelta(days=days_offset+init_delta+i))
        if(target_date.weekday()>=5):
            continue

        date_stamp = target_date.strftime('%Y-%m-%d')
        url_base = url_base_path + date_stamp + "/"
        agg_file_name = "aggregated_" + date_stamp + file_trail + ".csv"
        path = Path(agg_file_name)
        if path.exists(): 
            continue
        
        include_header=True
        for h in range(7,17):
            if h<10:
                hour_suffix = "0" + str(h)
            else:
                hour_suffix = str(h)

            file_name = date_stamp + file_trail + hour_suffix + ".csv"

            logger.info("downloading: %s", url_base + file_name)
            path = Path(file_name)
            try:
                if not path.exists(): 
                    urllib.request.urlretrieve(url_base + file_name, file_name)
            except:
                logger.error("error during download: %s, %r", url_base + file_name, sys.exc_info()[0])
                break

            if path.stat().st_size < 150:
                continue
            
            aggregate_hour_csv(file_name,include_header)
            include_header = False
            #time.sleep(1)
        else:
            continue

        break

if __name__ == '__main__':

    init_deltas = list(range(1,1081,30))
    p = Pool(6)
    p.map(monthly_downloader, init_deltas)

    logger.info("pool started")
