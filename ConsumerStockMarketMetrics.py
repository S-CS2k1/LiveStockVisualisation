import json
import boto3
import os, sys, threading
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when, length, col
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


consumer = KafkaConsumer(
    "stock-metric-topic",
    bootstrap_servers = "localhost:9092",
    auto_offset_reset = "earliest",
    group_id = "grp-1",
    consumer_timeout_ms = 5000
)

spark_sess = SparkSession.builder.appName("write_raw_data_to_hdfs").getOrCreate()


data = []

latest_data = {}

def consume_ETL():

    global latest_data

    for messages in consumer:
        print("Producer is sending data....")
        raw_data = json.loads(messages.value.decode())
        for i in range(len(raw_data["symbl"])):
            data.append((raw_data["symbl"][i], raw_data["trading_vol"][i], raw_data["quote"][i], raw_data["mrkt_cap"][i], raw_data["ipo"][i], raw_data["timestamp"][i]))

        df = spark_sess.createDataFrame(data, ["symbl", "trading_vol", "quote", "mrkt_cap", "ipo", "timestamp"])

        s3_bucket = boto3.client("s3")

        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .csv("raw_data")


        raw_data_file = [file for file in os.listdir("./raw_data") if file.startswith("part-")][0]


        my_bucket = s3_bucket.list_objects(Bucket="data-stream-bucket-shics")["Contents"]
        raw_data_cnt = 0
        for obj in my_bucket: raw_data_cnt += 1 if obj["Key"].startswith("raw_data/") else 0

        s3_bucket.upload_file(
            f"./raw_data/{raw_data_file}",
            "data-stream-bucket-shics",
            f"raw_data/raw_data_file_{raw_data_cnt}",
        )

        # os.remove("./raw_data")

        # ETL process

        df = df.withColumn("trading_vol", df["trading_vol"].cast(IntegerType()))
        df = df.withColumn("quote", df["quote"].cast(IntegerType()))
        df = df.withColumn("mrkt_cap", df["mrkt_cap"].cast(IntegerType()))
        df = df.withColumn("ipo", df["ipo"].cast(IntegerType()))

        df = df.withColumn("timestamp", when(length(df["timestamp"]) < 19, None).otherwise(df["timestamp"]))

        df = df.dropna(subset = ["timestamp"])

        def filteringBasedOnIQR(in_df, col_name):
            quantiles = in_df.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.01)
            iqr = quantiles[2] - quantiles[0]
            lb, ub = quantiles[0] - (1.5*iqr), quantiles[2] + (1.5*iqr)

            in_df = in_df.withColumn(
                col_name, 
                when((col(col_name) > lb) & (col(col_name) < ub), col(col_name)).otherwise(None)
            )
            return in_df.dropna(subset = [col_name])

        fields = df.dtypes

        for (col_name, type) in fields:
            if type != "string": df = filteringBasedOnIQR(df, col_name)


        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .csv("transformed_data")

        transformed_data_file = [file for file in os.listdir("./transformed_data") if file.startswith("part-")][0]

        s3_bucket.upload_file(
            f"./transformed_data/{transformed_data_file}",
            "data-stream-bucket-shics",
            f"transformed_data/transformed_data_file{raw_data_cnt}"
        )

        grouped_df = df.groupby("symbl").avg("trading_vol")
        latest_data = {row["symbl"]: row["avg(trading_vol)"] for row in grouped_df.collect()}

    print("Done")


# realtime visualization

fig, ax = plt.subplots()
ax.set_ylabel("Trading Volume")
ax.set_title("Live Trading Volume by Stock")

def update(frame):

    ax.clear()
    ax.set_ylabel("Trading Volume")
    ax.set_title("Live Trading Volume by Stock")

    if latest_data:
        names = list(latest_data.keys())
        vols = list(latest_data.values())
        ax.bar(names, vols)
        ax.set_ylim(0, max(vols)*1.2)

    return ax.patches


t = threading.Thread(target=consume_ETL, daemon=True)
t.start()


animation = FuncAnimation(fig, update, frames=50, interval=1000, blit=False)
plt.show()