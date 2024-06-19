import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def load_and_prep():
    stocks = spark.read.option("header", True).csv("s3://ron-levy-bucket-for-vi/stock_prices.csv", inferSchema=True)
    stocks = stocks.withColumn('date',F.to_date("date","M/d/yyyy"))
    stocks = stocks.orderBy(["ticker", "date"], ascending=True)
    windowSpec  = Window.partitionBy("ticker").orderBy("date")
    stocks = stocks.withColumn("prev_close", F.lag("close",1).over(windowSpec))
    return stocks

def save_df(df, output_dir):
    df.write.parquet(f"s3://ron-levy-bucket-for-vi/output/{output_dir}",mode="overwrite")


########## Code

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


stocks = load_and_prep()

### Q1
stocks = stocks.withColumn('return_pct', (stocks['close'] - stocks['prev_close']) / stocks['prev_close'])
q1_res = stocks.where("return_pct is not Null").groupBy("date").agg(
    F.avg("return_pct").alias("average_return")).orderBy("average_return", ascending=False)
save_df(q1_res, "Q1")


### Q2
stocks = stocks.withColumn('trade_freq_key', stocks['close'] * stocks['volume'])
q2_res = stocks.groupBy("ticker").agg(
    F.avg("trade_freq_key").alias("frequency")).orderBy("frequency", ascending=False)
save_df(q2_res, "Q2")


### Q3
stocks = stocks.withColumn('year', F.year(stocks['date']))
q3_res = stocks.groupBy(["ticker", "year"]).agg(
    F.stddev("return_pct").alias("standard_deviation")).orderBy("standard_deviation", ascending=False)
save_df(q3_res, "Q3")


### Q4
days_back = 30
stocks = stocks.withColumn('old_date', F.date_add(stocks['date'], -1 * days_back))

# self join
df1 = stocks.alias("df1")
df2 = stocks.alias("df2").selectExpr("date", "close as old_close","ticker")
joined_df = df1.join(df2,
                     (F.col("df1.old_date") == F.col("df2.date")) & (F.col("df1.ticker")==F.col("df2.ticker"))
                    )

new_col = f"return_{days_back}_days"
joined_df = joined_df.withColumn(new_col, (joined_df['close'] - joined_df['old_close']) / joined_df['old_close'])
q4_res = joined_df.selectExpr('df1.ticker', 'df1.date as end_date', "old_date as start_date", new_col).orderBy(
    new_col, ascending=False)
save_df(q4_res, "Q4")


job.commit()


"""
SQLs for views creation:

create or replace view most_traded_stock as
(
select * from 
    (select * from q2_most_freq_traded order by frequency desc) 
limit 1
)


create or replace view most_volatile as
(
select * from 
    (select * from q3_most_volatile order by standard_deviation desc) 
limit 1
)


create or replace view top_30_days_return as
(
select * from 
    (select * from q4_x_days_return order by return_30_days desc) 
limit 3
)


"""