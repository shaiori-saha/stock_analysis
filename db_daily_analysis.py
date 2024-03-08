import pandas as pd
import logging
from sqlalchemy import create_engine,types

from datetime import datetime
from datetime import timedelta
import time

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

QUERY='SELECT "Mnemonic", SUM("TradedVolume" * "StartPrice") AS ' + \
    '"TotalTrades" FROM "DBU_DAILY_AGGR_VIEW" WHERE "EntryDate">(CURRENT_DATE-1500)' +\
    'GROUP BY "Mnemonic" ORDER BY "TotalTrades" DESC LIMIT 250;'

if __name__ == '__main__':

    start_date = datetime(2020, 6, 24)

    dbConn = DB_ENGINE.connect()
    df = pd.read_sql(QUERY,dbConn)
    dbConn.close()
    df.to_csv('total_trades.csv')
    logger.info("completed")
