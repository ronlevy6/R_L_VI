import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import numpy as np


#### helper functions

def prep_data(params):
    stocks['date'] = stocks['date'].apply(pd.to_datetime)
    for col in ["open", "high", "low", "close", "volume"]:
        stocks[col] = stocks[col].astype(np.float32)
    stocks.sort_values(by=["ticker", "date"], ascending=[True, True], inplace=True)
    stocks.set_index(["ticker", "date"], inplace=True)
    return stocks


def calc_daily_return(stock_data: pd.DataFrame) -> pd.DataFrame:
    stocks_by_ticker = stock_data.groupby("ticker", sort=False)
    daily_return_d = dict()
    for ticker in stocks_by_ticker.groups:
        ticker_data = stocks_by_ticker.get_group(ticker)
        daily_return_d.update(ticker_data['close'].pct_change().to_dict())

    stock_data['daily_return'] = stock_data.index.map(daily_return_d)
    return stock_data


def calc_x_day_return(stock_data: pd.DataFrame, days_back: int) -> pd.DataFrame:
    def calc_return(curr_closing_val, old_closing_val):
        return (curr_closing_val - old_closing_val) / old_closing_val

    old_date_col = f'{days_back}_days_back'
    stock_data[old_date_col] = stock_data.index.get_level_values("date") - pd.Timedelta(days_back, unit="D")
    stocks_as_d = stock_data.to_dict(orient="index")
    old_closing_data = []

    for (ticker, curr_day), row_data in stocks_as_d.items():
        if (ticker, row_data[old_date_col]) in stocks_as_d:
            old_close_data = stocks_as_d[(ticker, row_data[old_date_col])]['close']
            old_closing_data.append(
                (ticker, curr_day, row_data[old_date_col], calc_return(row_data['close'], old_close_data)))

    new_col_name = f'return_{days_back}_days'
    old_closing_data_df = pd.DataFrame(old_closing_data,
                                       columns=['ticker', 'end_return_date', 'start_return_date', new_col_name])

    return old_closing_data_df, new_col_name


def write_to_file(df, output_dir, to_reset_index=True):
    if to_reset_index:
        df = df.reset_index()
    df_spark = spark.createDataFrame(df)
    dfg = DynamicFrame.fromDF(df_spark, glueContext, "dfg")
    outputGDF = glueContext.write_dynamic_frame.from_options(
        frame=dfg,
        connection_type="s3",
        connection_options={"path": f"s3://ron-levy-bucket-for-vi/output/{output_dir}"},
        format="parquet")


####### REAL CODE ################

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamicFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://ron-levy-bucket-for-vi/stock_prices.csv"]},
    format="csv",
    format_options={
        "withHeader": True,
        # "optimizePerformance": True,
    },
)

# read and prep input data
stocks = dynamicFrame.toDF().toPandas()
stocks = prep_data(stocks)
stocks = calc_daily_return(stocks)

# Q1
q1_res = stocks.dropna().groupby("date").agg({"daily_return": "mean"})
q1_res.rename(columns={"daily_return": "average_return"}, inplace=True)
write_to_file(q1_res, "Q1")

# Q2
pd.set_option('display.float_format', lambda x: '%.3f' % x)
stocks["trade_freq_key"] = stocks['close'] * stocks['volume']
mean_vol_per_stock = stocks.groupby("ticker").agg({"trade_freq_key": "mean"}).sort_values("trade_freq_key",
                                                                                          ascending=False)
mean_vol_per_stock.rename(columns={"trade_freq_key": "frequency"}, inplace=True)
mean_vol_per_stock.sort_values(by="frequency", ascending=False, inplace=True)
write_to_file(mean_vol_per_stock, "Q2")

# Q3
stocks['year'] = stocks.index.get_level_values('date').year
Q3_res = stocks.groupby(["ticker", "year"]).agg({"daily_return": "std"}).reset_index()
Q3_res.sort_values("daily_return", ascending=False, inplace=True)
Q3_res.rename(columns={"daily_return": "standard deviation"}, inplace=True)
Q3_res.sort_values(by="standard deviation", ascending=False, inplace=True)
write_to_file(Q3_res, "Q3", False)

# Q4
days_back = 30
Q4_res, new_col_name = calc_x_day_return(stocks, days_back=days_back)
Q4_res_top = Q4_res.sort_values(new_col_name, ascending=False)
write_to_file(Q4_res_top, "Q4", False)

job.commit()