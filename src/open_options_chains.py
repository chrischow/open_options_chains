# + -------------------- +
# | COLLECT OPTIONS DATA |
# + -------------------- +

# Imports
import os
import pendulum

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Set variables
TICKERS = ['FB', 'GOOG']

# Get variables
API_KEY = os.environ['API_KEY']
DB_PASSWORD = os.environ['APP_DB_PASS']

# Set arguments
us_east_tz = pendulum.timezone('America/New_York')
default_args = {
    'owner': 'chrischow',
    'start_date': datetime(2022, 1, 7, 7, 30, tzinfo=us_east_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Function to create table
def create_table(ticker):
    # Define Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Create table if it doesn't exist
    pg_hook.run(f"""
CREATE TABLE IF NOT EXISTS {ticker} (
    put_call VARCHAR(5) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    description VARCHAR(64) NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    last DOUBLE PRECISION,
    bid_size INTEGER,
    ask_size INTEGER,
    last_size INTEGER,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    open_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    total_volume INTEGER,
    quote_time BIGINT,
    volatility DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    open_interest INTEGER,
    time_value DOUBLE PRECISION,
    theoretical_value DOUBLE PRECISION,
    strike_price DOUBLE PRECISION,
    expiration_date BIGINT,
    dte INTEGER,
    PRIMARY KEY (symbol, quote_time)
)
""")

# Function to get data from TDA API
def extract_options_data_from_tda(ticker, ti):
    # Import modules
    import json
    import requests

    # Configure dates
    TIMEDELTA = timedelta(days=45)
    # start_date = datetime.today(tzinfo=us_east_tz)
    start_date = datetime.utcnow().replace(tzinfo=us_east_tz)
    end_date = start_date + TIMEDELTA
    
    # Configure request
    headers = {
        'Authorization': '',
    }

    params = (
        ('apikey', API_KEY),
        ('symbol', ticker),
        ('contractType', 'PUT'),
        ('strikeCount', '50'),
        ('range', 'ALL'),
        ('fromDate', start_date),
        ('toDate', end_date),
    )

    # Get data
    response = requests.get(
        'https://api.tdameritrade.com/v1/marketdata/chains',
        headers=headers,
        params=params
    )
    data = json.loads(response.content)

    # Push XCOM
    ti.xcom_push(key='raw_data', value=data)

# ---- PARSE DATA ---- #
def transform_options_data(ti):
    
    # Import modules
    import pandas as pd

    # Pull XCOM
    data = ti.xcom_pull(key='raw_data', task_ids=['extract_options_data_from_tda'])[0]

    # Define columns
    columns = ['putCall', 'symbol', 'description', 'exchangeName', 'bid', 'ask',
        'last', 'mark', 'bidSize', 'askSize', 'bidAskSize', 'lastSize',
        'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'totalVolume',
        'tradeDate', 'tradeTimeInLong', 'quoteTimeInLong', 'netChange',
        'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'openInterest',
        'timeValue', 'theoreticalOptionValue', 'theoreticalVolatility',
        'optionDeliverablesList', 'strikePrice', 'expirationDate',
        'daysToExpiration', 'expirationType', 'lastTradingDay', 'multiplier',
        'settlementType', 'deliverableNote', 'isIndexOption', 'percentChange',
        'markChange', 'markPercentChange', 'mini', 'inTheMoney', 'nonStandard']

    # Extract puts data
    puts = []
    dates = list(data['putExpDateMap'].keys())

    for date in dates:

        strikes = data['putExpDateMap'][date]

        for strike in strikes:
            puts += data['putExpDateMap'][date][strike]

    # Convert to dataframe
    puts = pd.DataFrame(puts, columns=columns)

    # Select columns
    puts = puts[['putCall', 'symbol', 'description', 'bid', 'ask', 'last', 'bidSize',
        'askSize', 'lastSize', 'highPrice', 'lowPrice', 'openPrice',
        'closePrice', 'totalVolume', 'quoteTimeInLong', 'volatility', 'delta',
        'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue',
        'theoreticalOptionValue', 'strikePrice', 'expirationDate',
        'daysToExpiration']]

    # Convert floats
    def conv_num(x):
        return pd.to_numeric(x.astype(str).str.replace('NaN|nan', '', regex=True))

    for col in ['bid', 'ask', 'last', 'highPrice', 'lowPrice', 'openPrice',
                'closePrice', 'volatility', 'delta', 'gamma', 'theta', 'vega',
                'rho', 'timeValue', 'theoreticalOptionValue', 'strikePrice']:
        puts[col] = conv_num(puts[col])

    # Specifically for puts delta: make it positive
    puts['delta'] = -puts['delta']

    # Convert strings
    def conv_str(x):
        return x.astype(str)

    for col in ['putCall', 'symbol', 'description']:
        puts[col] = conv_str(puts[col])

    # Convert integers
    def conv_int(x):
        return x.astype(int)

    for col in ['bidSize', 'askSize', 'lastSize', 'totalVolume', 'quoteTimeInLong',
                'openInterest', 'expirationDate', 'daysToExpiration']:
        puts[col] = conv_int(puts[col])

    # Fill missing values
    puts = puts.fillna(-99)

    # Rename columns
    puts = puts.rename(columns={
        'putCall': 'put_call',
        'bidSize': 'bid_size',
        'askSize': 'ask_size',
        'lastSize': 'last_size',
        'highPrice': 'high_price',
        'lowPrice': 'low_price',
        'openPrice': 'open_price',
        'closePrice': 'close_price',
        'totalVolume': 'total_volume',
        'quoteTimeInLong': 'quote_time',
        'openInterest': 'open_interest',
        'timeValue': 'time_value',
        'theoreticalOptionValue': 'theoretical_value',
        'strikePrice': 'strike_price',
        'expirationDate': 'expiration_date',
        'daysToExpiration': 'dte',
    })

    # Push XCOM
    ti.xcom_push(key='transformed_data', value=puts.to_dict('records'))

# ---- LOG INTO POSTGRES ---- #
def load_data_into_postgres(ticker, ti):
    
    # Import modules
    import pandas as pd

    # Define Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Pull XCOM
    puts = ti.xcom_pull(key='transformed_data', task_ids=['transform_options_data'])[0]
    puts = pd.DataFrame(puts)

    # Prepare insert query
    col_str = ', '.join(puts.columns.tolist())
    query_insert = f"INSERT INTO {ticker} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"

    # Convert to rows
    rows = list(puts.itertuples(index=False, name=None))
    for row in rows:
        pg_hook.run(query_insert % str(row))

# Function to create DAG
def create_dag(ticker, default_args):
    dag = DAG(
        dag_id=f'get_options_data_{ticker}',
        default_args=default_args,
        description=f'ETL for {ticker} options data',
        schedule_interval='*/30 8-21 * * 1-5',
        catchup=False,
        tags=['finance', 'options', ticker]
    )

    with dag:

        # Define operators
        task0_create_table = PythonOperator(
            task_id='create_table',
            python_callable=create_table,
            op_kwargs={'ticker': ticker}
        )

        task1_extract = PythonOperator(
            task_id='extract_options_data_from_tda',
            python_callable=extract_options_data_from_tda,
            op_kwargs={'ticker': ticker}
        )

        task2_transform = PythonOperator(
            task_id = 'transform_options_data',
            python_callable=transform_options_data
        )

        task3_load = PythonOperator(
            task_id='load_data_into_postgres',
            python_callable=load_data_into_postgres,
            op_kwargs={'ticker': ticker}
        )

        # Set up dependencies
        task0_create_table >> task1_extract >> task2_transform >> task3_load

        return dag


# Create DAGs
for ticker in TICKERS:
    globals()[f'get_options_data_{ticker}'] = create_dag(ticker, default_args)
    

    
    

