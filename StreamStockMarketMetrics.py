from kafka import KafkaProducer
import random
import datetime
import time
import json

def get_random_timestamp():
    strt_time = int(datetime.datetime(2023, 1, 1).timestamp())
    end_time = int(datetime.datetime(2025, 1, 1).timestamp())

    rand_ts = random.randint(strt_time, end_time)

    return str(datetime.datetime.fromtimestamp(rand_ts))

tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK.B", "JPM", "V", "JNJ", "UNH", "PG", "HD", "MA", "DIS", "PEP", "NFLX", "BAC", "KO"]

cnt = 0
data = {"symbl": ["symbl"], 
        "trading_vol": ["trading_vol"], 
        "quote": ["quote"], 
        "mrkt_cap": ["mrkt_cap"], 
        "ipo": ["ipo"], 
        "timestamp": ["timestamp"]
}
entry_types = {1: None, 2: "NaN", 3: "56.946"}

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)


for i in range(100):

    cnt += 1
    random_val = random.randint(1, 2)
    symbl_pos = random.randint(0, len(tickers) - 1)
    rand_pos = random.randint(1, 3)
    data["symbl"].append(tickers[symbl_pos])

    data["trading_vol"].append(str(random.randint(50000, 1000000) if random_val == 1 else entry_types[int(rand_pos)]))
    data["quote"].append(str(random.randint(1, 2000) if random_val == 1 else entry_types[int(rand_pos)]))
    data["mrkt_cap"].append(str(random.randint(400, 1600000) if random_val == 1 else entry_types[int(rand_pos)]))
    data["ipo"].append(str(random.randint(1, 2000) if random_val == 1 else entry_types[int(rand_pos)]))
    data["timestamp"].append(str(get_random_timestamp() if random_val == 1 else entry_types[int(rand_pos)]))

    if cnt == 5:

        producer.send("stock-metric-topic", data)

        cnt = 0
        data = {"symbl": ["symbl"], 
                "trading_vol": ["trading_vol"], 
                "quote": ["quote"], 
                "mrkt_cap": ["mrkt_cap"], 
                "ipo": ["ipo"], 
                "timestamp": ["timestamp"]
        }

        time.sleep(2)

producer.flush()