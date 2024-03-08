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

# dir|findstr "aggregated"|find /c /v ""
logger = logging.getLogger('root')
FORMAT = "[%(asctime)-15s - %(process)d][%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s"
logging.basicConfig(format=FORMAT, filename='daily_report.log', filemode='w', level=logging.DEBUG)

DB_FILE_PATH = "dbu_db.sqlite"
DB_TABLE = "DBU_DAILY_AGGR_VIEW"

def aggregate_day_csv(date):

    filename = "aggregated_" + date.strftime('%Y-%m-%d') + "_BINS_XETR.csv"
    path = Path(filename)

    if not path.exists(): 
        return
    
    if path.stat().st_size < 20:
        return

    logger.info("processing file: %s", filename)
    df_daily = pd.read_csv(filename, index_col=False, header=None, skiprows=1)
    df_daily.columns = ['Mnemonic', 'Hour', 'MinPrice', 'MaxPrice', \
                                'StartPrice', 'EndPrice', 'TradedVolume', 'NumTrades']

    grouped_traded_stocks = df_daily.groupby("Mnemonic")

    grouped_traded_stocks_startend = grouped_traded_stocks.agg({
        "Mnemonic" : lambda x: x.iloc[0],
        "MinPrice" : np.min,
        "MaxPrice" : np.max,
        "StartPrice" : lambda x: x.iloc[0],
        "EndPrice" : lambda x: x.iloc[-1],
        "TradedVolume" : np.sum,
        "NumTrades" : np.sum
    }
    )
    grouped_traded_stocks_startend['Date'] = date.strftime('%Y-%m-%d')
    logger.debug("aggregated stocks : %d", len(grouped_traded_stocks))

    with sqlite3.connect(DB_FILE_PATH) as conn:
        grouped_traded_stocks_startend.to_sql(DB_TABLE, conn, if_exists='append', index=False)

    return

def init_db():

    path = Path(DB_FILE_PATH)
    if path.exists(): 
        return

    with sqlite3.connect(DB_FILE_PATH) as conn:
        cur = conn.cursor()
        cur.execute('CREATE TABLE ' + DB_TABLE + ' (Mnemonic TEXT, ' + \
                                'MinPrice REAL, MaxPrice REAL, ' + \
                                    'StartPrice REAL, EndPrice REAL, TradedVolume INTEGER, ' + \
                                        'NumTrades INTEGER, Date TEXT)')
        conn.commit()

    return

if __name__ == '__main__':

    init_db()
    start_date = datetime(2020,1,10)
    for delta in range(1, 1080):
        target_date = (start_date - timedelta(days=delta))
        aggregate_day_csv(target_date)

    