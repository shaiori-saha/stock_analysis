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
from sqlalchemy import create_engine,types

# psql -U my_pg_user -W DBU_COMMON_STOCK
logger = logging.getLogger('root')
FORMAT = "[%(asctime)-15s - %(process)d][%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s"
logging.basicConfig(format=FORMAT, filename='daily_report.log', filemode='w', level=logging.DEBUG)

POSTGRES_USER='my_pg_user'
POSTGRES_PASSWORD='my_pg_pass'
POSTGRES_DB='DBU_COMMON_STOCK'
POSTGRES_PORT='5432'
POSTGRES_HOST='localhost'

DB_CONN_URL="postgresql://" + POSTGRES_USER + ":" + POSTGRES_PASSWORD + "@" \
                        + POSTGRES_HOST + ":" + POSTGRES_PORT + "/" + POSTGRES_DB

DB_ENGINE = create_engine(DB_CONN_URL)

DB_TABLE='DBU_DAILY_AGGR_VIEW'

DAILY_AGGR_CSV_PARENT_PATH = 'daily_aggregate\\'

def aggregate_day_csv(date):

    filename = DAILY_AGGR_CSV_PARENT_PATH + \
                    "aggregated_" + date.strftime('%Y-%m-%d') + "_BINS_XETR.csv"
    path = Path(filename)

    if not path.exists(): 
        print("file not found.")
        return
    
    if path.stat().st_size < 20:
        print("file empty." + str(date))
        return

    logger.info("processing file: %s", filename)
    df_daily = pd.read_csv(filename, index_col=False, header=None, skiprows=1)
    df_daily.columns = ['Mnemonic', 'Hour', 'MinPrice', 'MaxPrice', \
                                'StartPrice', 'EndPrice', 'TradedVolume', 'NumTrades']

    grouped_traded_stocks = df_daily.groupby("Mnemonic")

    grouped_traded_stocks_startend = grouped_traded_stocks.agg({
        "Mnemonic" : lambda x: x.iloc[0],
        "MinPrice" : min,
        "MaxPrice" : max,
        "StartPrice" : lambda x: x.iloc[0],
        "EndPrice" : lambda x: x.iloc[-1],
        "TradedVolume" : sum,
        "NumTrades" : sum
    }
    )
    grouped_traded_stocks_startend['EntryDate'] = date.strftime('%Y-%m-%d')
    logger.debug("aggregated stocks : %d", len(grouped_traded_stocks))

    grouped_traded_stocks_startend.to_sql(DB_TABLE, DB_ENGINE, \
                chunksize=500, if_exists='append', index=False, \
                    dtype={
                        "Mnemonic" : types.Text,
                        "MinPrice" : types.REAL,
                        "MaxPrice" : types.REAL,
                        "StartPrice" : types.REAL,
                        "EndPrice" : types.REAL,
                        "TradedVolume" : types.Integer,
                        "NumTrades": types.Integer,
                        "EntryDate": types.Date
                    })

    return

if __name__ == '__main__':
    print("start")
    start_date = datetime(2020, 6, 24)
    for delta in range(1, 1080):
        target_date = (start_date - timedelta(days=delta))
        aggregate_day_csv(target_date)
    logger.info("completed")

    