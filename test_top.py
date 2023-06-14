import random
import time

from scaled.client import Client

def sleep_print(sec: int):
    time.sleep(random.random())
    return sec

def main():

    address = "tcp://127.0.0.1:2345"
    client = Client(address=address)

    tasks = [random.randint(0, 101) for _ in range(20)]
    futures = [client.submit(sleep_print, a) for a in tasks]
    results = [future.result() for future in futures]
    print(results)
    client.disconnect()


if __name__ == "__main__":
    main()
