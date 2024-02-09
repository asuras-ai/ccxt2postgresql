import ccxt
import pandas as pd
import time
import psycopg2
from datetime import datetime
from sqlalchemy import inspect, create_engine
import dontshare_config as ds
import ccxt_exchanges as ex

# Connect to TimescaleDB
host = '10.66.7.110'
port = 5432
user = 'postgres'
password = ds.db_pass
database = 'postgres'

# using psycopg2
conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
cursor = conn.cursor()

# using sqlalchemy 
engine = create_engine(f'postgresql+psycopg2://{user}:{ds.db_pass}@{host}/{database}')
insp = inspect(engine)

# Input parameters:
exchanges = (ex.bybit, ex.binance, ex.coinbase) # open ccxt_exchanges.py for supported exchanges
symbols = ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOGEUSDT', 'TRXUSDT', 'LINKUSDT', 'DOT', 'SHIBUSDT') 
timeframes = ('4h', '1d', '15m', '1h') #, '4h', '1d', etc.

def fetch_binance_ohlcv(exchange, symbol, timeframe, since):
    
    ohlcv = []
    limit = 500

    while True:
        data = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not data:
            break
        ohlcv.extend(data)
        since = data[-1][0] + 1  # Set the next timestamp to avoid duplicates
        time.sleep(3)  # Wait for 3 seconds before the next request

    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

# Create table if not exists
def create_table(table_name):
   create_table_query = f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
      time TIMESTAMP WITHOUT TIME ZONE UNIQUE NOT NULL,
      open float NOT NULL,
      high float NOT NULL,
      low float NOT NULL,
      close float NOT NULL,
      volume float NOT NULL
      );
   """
   cursor.execute(create_table_query)
   conn.commit()
   print('Table ' + table_name + ' created if new.')

if __name__ == "__main__":
    for exchange in exchanges:
        print('Starting exchange ' + str(exchange))
        for symbol in symbols:
            print('Starting symbol ' + symbol)
            for timeframe in timeframes:
                print('Starting timeframe ' + timeframe)
                # create table name from input variables
                table_name = str(exchange) + '_' + symbol + '_' + timeframe
                table_name = table_name.lower()

                # check if table exists and create it if not. Get newest entry in existing for efficient scraping.
                if insp.has_table(table_name):
                    cursor.execute("SELECT time FROM bybit_btcusdt_1d ORDER BY time DESC LIMIT 1;")
                    newest_entry = cursor.fetchone()[0]
                    since = datetime.timestamp(newest_entry) *1000
                    since = int(since)
                    print('Table ' + table_name + ' exists. Start scraping at ' + str(newest_entry))
                else:
                    print('Create table ' + table_name)
                    create_table(table_name)
                    since = 473385600000  # Unix timestamp for 1984-01-01 00:00:00

                # Scrape data from exchange
                time.sleep(3)
                try:
                    historical_data = fetch_binance_ohlcv(exchange, symbol, timeframe, since)

                    # Insert data into TimeScaleDB table
                    for _, row in historical_data.iterrows():
                        try:
                            insert_query = f"""
                            INSERT INTO {table_name} (time, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, tuple(row))
                        except:
                            continue

                    conn.commit()
                except Exception as e:
                    print(e)
                    continue
    cursor.close()
    conn.close()
    print('Finished. Connection to database closed.') 