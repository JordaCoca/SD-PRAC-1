import requests
import time

URL = "http://127.0.0.1:8080/metrics"

while True:
    try:
        r = requests.get(URL)
        print(r.json())
    except:
        print("No metrics yet...")

    time.sleep(2)