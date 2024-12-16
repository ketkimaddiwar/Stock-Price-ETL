# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='compute_wh',
        database='dev'
    )
    # Create a cursor object
    return conn.cursor()


@task
def load_data(records, cur):
    target_table = "dev.raw_data.stock_table"
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE OR REPLACE TABLE {target_table} (date datetime primary key,"\
                    "open float, high float, low float, close float, volume float,"\
                    "symbol string);")
        for r in records: # we want records except the first one
            symbol = str(r[0])
            date = r[1]
            open = float(r[2])
            high = float(r[3])
            low = float(r[4])
            close = float(r[5])
            volume = int(r[6])
            # use parameterized INSERT INTO to handle some special characters such as '
            sql = f"INSERT INTO {target_table} (date, open, high, low, close, volume, symbol) VALUES ('{date}',{open},{high},{low},{close},{volume},'{symbol}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

@task
def return_last_90d_price(symbol, vantage_api_key):
  """
   - return the last 90 days of the stock prices of symbol as a list of json strings
  """
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
  r = requests.get(url)
  data = r.json()
  results = []   # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
  for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
    results.append((symbol,d,*data["Time Series (Daily)"][d].values()))
  results = results[:90]
  return results

with DAG(
    dag_id = 'Stock_prices',
    start_date = datetime(2024,9,27),
    catchup=False,
    tags=['ETL'],
    schedule = '00 14 * * *'
) as dag:
    target_table = "dev.raw_data.stock_table"
    vantage_api_key = Variable.get("vantage_api_key")
    cur = return_snowflake_conn()
    records = return_last_90d_price('AAPL', vantage_api_key)
    load_data(records, cur)