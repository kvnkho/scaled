import random

from scaled.client import Client
from scaled.cluster.combo import SchedulerClusterCombo

from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger 

import logging

def sleep_print(sec: int):
    setup_logger()
    logging.info("test")
    return sec * 1

def main():
    setup_logger()

    address = "tcp://127.0.0.1:2345"
    network_log_address = "tcp://127.0.0.1:3456"

    cluster = SchedulerClusterCombo(address=address, 
    n_workers=2, per_worker_queue_size=2, event_loop="builtin",
    network_log_address=network_log_address)
    client = Client(address=address)

    tasks = [random.randint(0, 101) for _ in range(10)]

    with ScopedLogger(f"scaled submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, a) for a in tasks]

    with ScopedLogger(f"scaled gather {len(futures)} results"):
        results = [future.result() for future in futures]

if __name__ == "__main__":
    main()